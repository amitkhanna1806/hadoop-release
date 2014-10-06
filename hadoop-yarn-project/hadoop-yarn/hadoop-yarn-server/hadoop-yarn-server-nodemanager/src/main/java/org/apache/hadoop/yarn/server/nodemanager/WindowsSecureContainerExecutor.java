/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.WinutilsProcessStub;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

/**
 * Windows secure container executor (WSCE).
 * This class offers a secure container executor on Windows, similar to the LinuxContainerExecutor
 * As the NM does not run on a high privileged context, this class delegates elevated operations
 * to the helper hadoopwintuilsvc, implemented by the winutils.exe running as a service.
 * JNI and LRPC is used to communicate with the privileged service.
 */
public class WindowsSecureContainerExecutor extends DefaultContainerExecutor {
  
  private static final Log LOG = LogFactory
      .getLog(WindowsSecureContainerExecutor.class);
  
  public static final String LOCALIZER_PID_FORMAT = "STAR_LOCALIZER_%s";

  /**
   * A shell script wrapper builder for WSCE.  
   * Overwrites the default behavior to remove the creation of the PID file in the script wrapper.
   * WSCE creates the pid file as part of launching the task in winutils
   */
  private class WindowsSecureWrapperScriptBuilder 
    extends LocalWrapperScriptBuilder {

    public WindowsSecureWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
    }

    @Override
    protected void writeLocalWrapperScript(Path launchDst, Path pidFile, PrintStream pout) {
      pout.format("@call \"%s\"", launchDst);
    }
  }

  /**
   * This is a skeleton file system used to elevate certain operations.
   * WSCE has to create container dirs under local/userchache/$user but
   * this dir itself is owned by $user, with chmod 750. As ther NM has no
   * write access, it must delegate the write operations to the privileged
   * hadoopwintuilsvc.
   */
  private static class ElevatedFileSystem extends DelegateToFileSystem {

    /**
     * This overwrites certain RawLocalSystem operations to be performed by a privileged process.
     * 
     */
    private static class ElevatedRawLocalFilesystem extends RawLocalFileSystem {
      
      @Override
      protected boolean mkOneDir(File p2f) throws IOException {
        Path path = new Path(p2f.getAbsolutePath());
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:mkOneDir: %s", path));
        }
        boolean ret = false;

        // File.mkdir returns false, does not throw. Must mimic it.
        try {
          NativeIO.Elevated.mkdir(path);
          ret = true;
        }
        catch(Throwable e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("EFS:mkOneDir: %s", 
                org.apache.hadoop.util.StringUtils.stringifyException(e)));
          }
        }
        return ret;
      }
      
      @Override
      public void setPermission(Path p, FsPermission permission) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:setPermission: %s %s", p, permission));
        }
        NativeIO.Elevated.chmod(p, permission.toShort());
      }
      
      @Override
      public void setOwner(Path p, String username, String groupname) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:setOwner: %s %s %s", p, username, groupname));
        }
        NativeIO.Elevated.chown(p, username, groupname);
      }
      
      @Override
      protected OutputStream createOutputStream(Path f, boolean append) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:create: %s %b", f, append));
        }
        return NativeIO.Elevated.create(f, append); 
      }
      
      @Override
      public boolean delete(Path p, boolean recursive) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("EFS:delete: %s %b", p, recursive));
        }
        
        // The super delete uses the FileUtil.fullyDelete, but we cannot rely on that
        // because we need to use the elevated operations to remove the files
        //
        File f = pathToFile(p);
        if (!f.exists()) {
          //no path, return false "nothing to delete"
          return false;
        }
        else if (f.isFile()) {
          return NativeIO.Elevated.deleteFile(p);
        } 
        else if (f.isDirectory()) {
          
          // This is a best-effort attempt. There are race conditions in that
          // child files can be created/deleted after we snapped the list. 
          // No need to protect against that case.
          File[] files = FileUtil.listFiles(f);
          int childCount = files.length;
          
          if (recursive) {
            for(File child:files) {
              if (delete(new Path(child.getPath()), recursive)) {
                --childCount;
              }
            }
          }
          if (childCount == 0) {
            return NativeIO.Elevated.deleteDirectory(p);
          } 
          else {
            throw new IOException("Directory " + f.toString() + " is not empty");
          }
        }
        else {
          // This can happen under race conditions if an external agent is messing with the file type between IFs
          throw new IOException("Path " + f.toString() + " exists, but is neither a file nor a directory");
        }
      }
    }

    protected ElevatedFileSystem() throws IOException, URISyntaxException {
      super(FsConstants.LOCAL_FS_URI,
          new ElevatedRawLocalFilesystem(), 
          new Configuration(),
          FsConstants.LOCAL_FS_URI.getScheme(),
          false);
    }
  }
  
  private static class WintuilsProcessStubExecutor implements Shell.CommandExecutor {
    private WinutilsProcessStub processStub;
    private StringBuilder output = new StringBuilder();
    private int exitCode;
    
    private enum State {
      INIT,
      RUNNING,
      COMPLETE
    };
    
    private State state;;
    
    private final String cwd;
    private final String jobName;
    private final String userName;
    private final String pidFile;
    private final String cmdLine;

    public WintuilsProcessStubExecutor(
        String cwd, 
        String jobName, 
        String userName, 
        String pidFile,
        String cmdLine) {
      this.cwd = cwd;
      this.jobName = jobName;
      this.userName = userName;
      this.pidFile = pidFile;
      this.cmdLine = cmdLine;
      this.state = State.INIT;
    }    
    
    private void assertComplete() throws IOException {
      if (state != State.COMPLETE) {
        throw new IOException("Process is not complete");
      }
    }
    
    public String getOutput () throws IOException {
      assertComplete();
      return output.toString();
    }
    
    public int getExitCode() throws IOException {
      assertComplete();
      return exitCode;
    }
    
    public void validateResult() throws IOException {
      assertComplete();
      if (0 != exitCode) {
        LOG.warn(output.toString());
        throw new IOException("Processs exit code is:" + exitCode);
      }
    }
    
    private Thread startStreamReader(final InputStream stream) throws IOException {
      Thread streamReaderThread = new Thread() {
        
        @Override
        public void run() {
          try
          {
            BufferedReader rdr = new BufferedReader(
                new InputStreamReader(stream));
            String line = rdr.readLine();
            while((line != null) && !isInterrupted()) {
              synchronized(output) {
                output.append(line);
                output.append(System.getProperty("line.separator"));
              }
              line = rdr.readLine();
            }
          }
          catch(Throwable t) {
            LOG.error("Error occured reading the process stdout", t);
          }
        }
      };
      streamReaderThread.start();
      return streamReaderThread;
    }

    public void execute() throws IOException {
      if (state != State.INIT) {
        throw new IOException("Process is already started");
      }
      processStub = NativeIO.createTaskAsUser(cwd, jobName, userName, pidFile, cmdLine);
      state = State.RUNNING;

      Thread stdOutReader = startStreamReader(processStub.getInputStream());
      Thread stdErrReader = startStreamReader(processStub.getErrorStream());
      
      try {
        processStub.resume();
        processStub.waitFor();
        stdOutReader.join();
        stdErrReader.join();
      }
      catch(InterruptedException ie) {
        throw new IOException(ie);
      }
      
      exitCode = processStub.exitValue();
      state = State.COMPLETE;
    }

    @Override
    public void close() {
      if (processStub != null) {
        processStub.dispose();
      }
    }
  }

  private String nodeManagerGroup;
  
  /** 
   * Permissions for user WSCE dirs.
   */
  static final short DIR_PERM = (short)0750;  
  
  public WindowsSecureContainerExecutor() throws IOException, URISyntaxException {
    super(FileContext.getFileContext(new ElevatedFileSystem(), new Configuration()));
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    nodeManagerGroup = conf.get(YarnConfiguration.NM_WINDOWS_SECURE_CONTAINER_GROUP);
  }
  
  @Override
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration conf) {
    File f = new File(command);
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("getRunCommand: %s exists:%b", command, f.exists()));
    }
    return new String[] { Shell.WINUTILS, "task", "createAsUser", groupId, userName,
        pidFile.toString(), "cmd /c " + command };
  }
  
  @Override
  protected LocalWrapperScriptBuilder getLocalWrapperScriptBuilder(
      String containerIdStr, Path containerWorkDir) {
   return  new WindowsSecureWrapperScriptBuilder(containerWorkDir);
  }
  
  @Override
  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("copyFile: %s -> %s owner:%s", src.toString(), dst.toString(), owner));
    }
    NativeIO.Elevated.copy(src,  dst, true);
    NativeIO.Elevated.chown(dst, owner, nodeManagerGroup);
  }

  @Override
  protected void createDir(Path dirPath, FsPermission perms,
      boolean createParent, String owner) throws IOException {
    
    // WSCE requires dirs to be 750, not 710 as DCE.
    // This is similar to how LCE creates dirs
    //
    perms = new FsPermission(DIR_PERM);
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("createDir: %s perm:%s owner:%s", dirPath.toString(), perms.toString(), owner));
    }
    
    super.createDir(dirPath, perms, createParent, owner);
    lfs.setOwner(dirPath, owner, nodeManagerGroup);
  }

  @Override
  protected void setScriptExecutable(Path script, String owner) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("setScriptExecutable: %s owner:%s", script.toString(), owner));
    }
    super.setScriptExecutable(script, owner);
    NativeIO.Elevated.chown(script, owner, nodeManagerGroup);
  }

  @Override
  public Path localizeClasspathJar(Path classPathJar, Path pwd, String owner) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("localizeClasspathJar: %s %s o:%s", classPathJar, pwd, owner));
    }
    createDir(pwd,  new FsPermission(DIR_PERM), true, owner);
    String fileName = classPathJar.getName();
    Path dst = new Path(pwd, fileName);
    NativeIO.Elevated.move(classPathJar, dst, true);
    NativeIO.Elevated.chown(dst, owner, nodeManagerGroup);
    return dst;
  }

 @Override
 public void startLocalizer(Path nmPrivateContainerTokens,
     InetSocketAddress nmAddr, String user, String appId, String locId,
     LocalDirsHandlerService dirsHandler) throws IOException,
     InterruptedException {
   
     List<String> localDirs = dirsHandler.getLocalDirs();
     List<String> logDirs = dirsHandler.getLogDirs();
     
     Path classpathJarPrivateDir = dirsHandler.getLocalPathForWrite(ResourceLocalizationService.NM_PRIVATE_DIR);
     createUserLocalDirs(localDirs, user);
     createUserCacheDirs(localDirs, user);
     createAppDirs(localDirs, user, appId);
     createAppLogDirs(appId, logDirs, user);
     

     // TODO: Why pick first app dir. The same in LCE why not random?
     Path appStorageDir = getFirstApplicationDir(localDirs, user, appId);
     
     String tokenFn = String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
     Path tokenDst = new Path(appStorageDir, tokenFn);
     copyFile(nmPrivateContainerTokens, tokenDst, user);

     File cwdApp = new File(appStorageDir.toString());
     if (LOG.isDebugEnabled()) {
       LOG.debug(String.format("cwdApp: %s", cwdApp));
     }
     
     List<String> command ;

     command = new ArrayList<String>();

   //use same jvm as parent
     File jvm = new File(new File(System.getProperty("java.home"), "bin"), "java.exe");
     command.add(jvm.toString());
     
     Path cwdPath = new Path(cwdApp.getPath());
     
     // Build a temp classpath jar. See ContainerLaunch.sanitizeEnv().
     // Passing CLASSPATH explicitly is *way* too long for command line.
     String classPath = System.getProperty("java.class.path");
     Map<String, String> env = new HashMap<String, String>(System.getenv());
     String classPathJar = FileUtil.createJarWithClassPath(classPath, 
         classpathJarPrivateDir, cwdPath, env);
     classPathJar = localizeClasspathJar(
         new Path(classPathJar), cwdPath, user).toString();
     command.add("-classpath");
     command.add(classPathJar);

     String javaLibPath = System.getProperty("java.library.path");
     if (javaLibPath != null) {
       command.add("-Djava.library.path=" + javaLibPath);
     }
     
     ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr, localDirs);
     
     String cmdLine = StringUtils.join(command, " ");
     
     String localizerPid = String.format(LOCALIZER_PID_FORMAT, locId);
     
     WintuilsProcessStubExecutor stubExecutor = new WintuilsProcessStubExecutor(
         cwdApp.getAbsolutePath(), 
         localizerPid, user, "nul:", cmdLine);
     try {
       stubExecutor.execute();
       stubExecutor.validateResult();
     }
     finally {
       stubExecutor.close();
       try
       {
         killContainer(localizerPid, Signal.KILL);
       }
       catch(Throwable e) {
         LOG.warn(String.format("An exception occured during the cleanup of localizer job %s:\n%s", 
             localizerPid, org.apache.hadoop.util.StringUtils.stringifyException(e)));
       }
     }
   }
 
   @Override
   protected CommandExecutor buildCommandExecutor(String wrapperScriptPath, String containerIdStr,
     String userName, Path pidFile, Configuration conf, File wordDir, Map<String, String> environment) 
     throws IOException {

     return new WintuilsProcessStubExecutor(
         wordDir.toString(),
         containerIdStr, userName, pidFile.toString(), "cmd /c " + wrapperScriptPath);
   }
   
   @Override
   protected void killContainer(String pid, Signal signal) throws IOException {
     NativeIO.Elevated.killTask(pid);
   }
}


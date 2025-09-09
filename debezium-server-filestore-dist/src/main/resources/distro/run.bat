@echo off
SET PATH_SEP=;
SET JAVA_BINARY=%JAVA_HOME%\bin\java

for %%i in (debezium-server-*runner.jar) do set RUNNER=%%~i
echo Using runner: %RUNNER%
SET LIB_PATH=lib\*
@REM Configuration files and directories that need to be on the classpath
SET LIB_CONFIG_PATH=config\lib

if "%RUNNER%"=="" (
  echo Error: No runner JAR found. Please ensure the Quarkus runner JAR is built.
  exit /b 1
)

IF %ENABLE_DEBEZIUM_SCRIPTING%=="true" LIB_PATH=%LIB_PATH%%PATH_SEP%lib_opt\*

call "%JAVA_BINARY%" %DEBEZIUM_OPTS% %JAVA_OPTS% -cp %RUNNER%%PATH_SEP%%LIB_CONFIG_PATH%%PATH_SEP%%LIB_PATH% io.debezium.server.Main

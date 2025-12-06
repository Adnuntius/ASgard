@echo off
setlocal

set DIR=%~dp0
set APP_HOME=%DIR%..
set CLASSPATH=%APP_HOME%\gradle\wrapper\gradle-wrapper.jar

if defined JAVA_HOME (
    set JAVA_EXE=%JAVA_HOME%\bin\java.exe
    if not exist "%JAVA_EXE%" (
        echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
        exit /b 1
    )
) else (
    set JAVA_EXE=java
)

"%JAVA_EXE%" %JAVA_OPTS% %GRADLE_OPTS% -Dorg.gradle.appname=gradlew -classpath "%CLASSPATH%" org.gradle.wrapper.GradleWrapperMain %*

@ECHO OFF
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION
SET "CURRENT_DIR=%CD%"
PUSHD %~dp0

SET "JAVA_HOME=(JVMのインストール先)"
SET "AWS_ACCESS_KEY_ID=(アクセスキーID)"
SET "AWS_SECRET_ACCESS_KEY=(シークレットアクセスキー)"
SET "VERSION=0.1.0"

%JAVA_HOME%\bin\java.exe -jar amazon-connect-real-time-streaming-%VERSION%-jar-with-dependencies.jar

POPD
ENDLOCAL

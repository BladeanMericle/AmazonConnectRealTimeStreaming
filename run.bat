@ECHO OFF
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION
SET "CURRENT_DIR=%CD%"
PUSHD %~dp0

SET "JAVA_HOME=(Java�̃C���X�g�[����)"
SET "AWS_ACCESS_KEY_ID=(�A�N�Z�X�L�[ ID)"
SET "AWS_SECRET_ACCESS_KEY=(�V�[�N���b�g�A�N�Z�X�L�[)"
SET "VERSION=0.0.1"

%JAVA_HOME%\bin\java.exe -jar amazon-connect-real-time-streaming-%VERSION%-jar-with-dependencies.jar

POPD
ENDLOCAL

@echo off
REM host.bat — thin wrapper so Chrome's native messaging (which needs a
REM directly-executable path, not a script requiring an interpreter) can
REM launch our Node.js host script.
REM
REM %~dp0 expands to this .bat file's own directory (with trailing backslash),
REM so this works regardless of where the project folder is placed.

node "%~dp0host.js" %*

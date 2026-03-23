' Claude Transcript ETL - Background Loop (Windows)
' Runs etl.py every 30 minutes silently (no console window).
' Started automatically via shell:startup launcher, or run directly:
'   wscript.exe etl-loop.vbs

Dim WshShell, fso, etlDir, pythonExe, etlScript, logFile, intervalMs

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

etlDir = fso.GetParentFolderName(WScript.ScriptFullName)
etlScript = fso.BuildPath(etlDir, "etl.py")
logFile = fso.BuildPath(etlDir, "logs\etl-loop.log")

' Find python on PATH
On Error Resume Next
pythonExe = WshShell.RegRead("HKLM\SOFTWARE\Python\PythonCore\3.13\InstallPath\ExecutablePath")
On Error GoTo 0
If pythonExe = "" Then pythonExe = "python"

' 30 minutes in milliseconds
intervalMs = 30 * 60 * 1000

' Ensure logs directory exists
If Not fso.FolderExists(fso.BuildPath(etlDir, "logs")) Then
    fso.CreateFolder fso.BuildPath(etlDir, "logs")
End If

' Main loop
Do While True
    Dim cmd
    cmd = """" & pythonExe & """ """ & etlScript & """ >> """ & logFile & """ 2>&1"
    WshShell.Run "cmd /c " & cmd, 0, True
    WScript.Sleep intervalMs
Loop

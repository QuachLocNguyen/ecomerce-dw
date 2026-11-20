import subprocess

ps_content = """[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.AnalysisServices') | Out-Null
$server = New-Object Microsoft.AnalysisServices.Server
$server.Connect('.\\SQLEXPRESS')
$db = $server.Databases['EcommerceDW_SSAS']

foreach ($dim in $db.Dimensions) {
    if ($dim.State -ne 'Processed') {
        Write-Host "Processing $($dim.Name)..."
        try {
            $dim.Process([Microsoft.AnalysisServices.ProcessType]::ProcessFull)
            Write-Host "  -> $($dim.Name) OK!"
        } catch {
            Write-Host "  -> FAILED $($dim.Name): $($_.Exception.Message)"
        }
    }
}

foreach ($cube in $db.Cubes) {
    Write-Host "Processing Cube $($cube.Name)..."
    try {
        $cube.Process([Microsoft.AnalysisServices.ProcessType]::ProcessFull)
        Write-Host "  -> Cube $($cube.Name) OK!"
    } catch {
        Write-Host "  -> FAILED Cube $($cube.Name): $($_.Exception.Message)"
    }
}
"""

with open("check_process.ps1", "w", encoding="utf-8") as f:
    f.write(ps_content)

res = subprocess.run(["powershell", "-ExecutionPolicy", "Bypass", "-File", "check_process.ps1"], capture_output=True, text=True)
print("OUT:", res.stdout)

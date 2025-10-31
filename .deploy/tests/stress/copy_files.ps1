Push-Location $PSScriptRoot
Push-Location ../../..

try {
    deactivate
} catch {
}
& "./tests/stress/.venv/scripts/activate.ps1"
pip freeze > ./tests/stress/requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/tests/stress/output"
$exclude_folders = @(".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = $publish_folder + "/shared"
$publish_infrastructure_folder = $publish_folder + "/infrastructure"

python $copy_folder_script ./tests/stress $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./infrastructure $publish_infrastructure_folder -e $exclude_folders
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder

Pop-Location
Pop-Location
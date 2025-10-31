Push-Location $PSScriptRoot
Push-Location ../..

try {
    deactivate
} catch {
}
& "./schedule/handlers/.venv/scripts/activate.ps1"
pip freeze > ./schedule/handlers/requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/schedule_handlers/output"
$exclude_folders = @(".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = $publish_folder + "/shared"
$publish_infrastructure_folder = $publish_folder + "/infrastructure"

python $copy_folder_script ./schedule/handlers $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./schedule/shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./infrastructure $publish_infrastructure_folder -e $exclude_folders
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder

python -m venv $publish_folder/.venv
& "$publish_folder/.venv/Scripts/activate.ps1"
pip install -r $publish_folder/requirements.txt
pip install "faststream[cli]"
pip freeze > $publish_folder/requirements.txt
deactivate
Remove-Item -Path $publish_folder/.venv -Recurse

Pop-Location
Pop-Location
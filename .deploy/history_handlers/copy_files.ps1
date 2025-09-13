Push-Location $PSScriptRoot
Push-Location ../..

try {
    deactivate
} catch {
}
& "./history/handlers/.venv/scripts/activate.ps1"
pip freeze > ./history/handlers/requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/history_handlers/output"
$exclude_folders = @(".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = Join-Path -Path $publish_folder -ChildPath "shared"
$publish_infrastructure_folder = Join-Path -Path $publish_folder -ChildPath "infrastructure"

python $copy_folder_script ./history/handlers $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./history/shared $publish_shared_folder -e $exclude_folders
New-Item -Path $publish_infrastructure_folder -ItemType Directory
Copy-Item -Path ./infrastructure/rabbitdefinitioncompleted.py -Destination $publish_infrastructure_folder
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
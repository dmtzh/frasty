Push-Location $PSScriptRoot
Push-Location ../..

try {
    deactivate
} catch {
}
& "./tasks/webapi/.venv/scripts/activate.ps1"
pip freeze > ./tasks/webapi/requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/tasks_webapi/output"
$exclude_folders = @(".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = Join-Path -Path $publish_folder -ChildPath "shared"
$publish_infrastructure_folder = Join-Path -Path $publish_folder -ChildPath "infrastructure"

python $copy_folder_script ./tasks/webapi $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./tasks/shared $publish_shared_folder -e $exclude_folders
New-Item -Path $publish_infrastructure_folder -ItemType Directory
Copy-Item -Path ./infrastructure/rabbitmiddlewares.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitdefinitioncompleted.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitruntask.py -Destination $publish_infrastructure_folder
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder

Pop-Location
Pop-Location
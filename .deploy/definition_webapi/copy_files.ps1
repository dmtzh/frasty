Push-Location $PSScriptRoot
Push-Location ../..

try {
    deactivate
} catch {
}
& "./definition/webapi/.venv/scripts/activate.ps1"
pip freeze > ./definition/webapi/requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/definition_webapi/output"
$exclude_folders = @(".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = Join-Path -Path $publish_folder -ChildPath "shared"
$publish_stepdefinitions_folder = Join-Path -Path $publish_folder -ChildPath "stepdefinitions"
$publish_infrastructure_folder = Join-Path -Path $publish_folder -ChildPath "infrastructure"

python $copy_folder_script ./definition/webapi $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./definition/shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./stepdefinitions $publish_stepdefinitions_folder -e $exclude_folders
New-Item -Path $publish_infrastructure_folder -ItemType Directory
Copy-Item -Path ./infrastructure/rabbitmiddlewares.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitcompletestep.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitdefinitioncompleted.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitrunstep.py -Destination $publish_infrastructure_folder
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder
python $copy_folder_script ./../html_sources $publish_folder/html_sources

Pop-Location
Pop-Location
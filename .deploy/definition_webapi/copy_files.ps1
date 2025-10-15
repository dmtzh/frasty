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

$publish_shared_folder = $publish_folder + "/shared"
$publish_stepdefinitions_folder = $publish_folder + "/stepdefinitions"
$publish_stephandlers_folder = $publish_folder + "/stephandlers"
$publish_infrastructure_folder = $publish_folder + "/infrastructure"

python $copy_folder_script ./definition/webapi $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./definition/shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./stepdefinitions $publish_stepdefinitions_folder -e $exclude_folders
$step_handlers_with_definitions = @("sendtoviberchannel", "getcontentfromjson")
foreach ($step_child_folder in $step_handlers_with_definitions) {
    New-Item -Path $publish_stephandlers_folder/$step_child_folder -ItemType Directory
    Copy-Item -Path ./stephandlers/$step_child_folder/definition.py -Destination $publish_stephandlers_folder/$step_child_folder
}
New-Item -Path $publish_infrastructure_folder -ItemType Directory
Copy-Item -Path ./infrastructure/rabbitmiddlewares.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitcompletestep.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitdefinitioncompleted.py -Destination $publish_infrastructure_folder
Copy-Item -Path ./infrastructure/rabbitrunstep.py -Destination $publish_infrastructure_folder
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder
python $copy_folder_script ./../html_sources $publish_folder/html_sources

Pop-Location
Pop-Location
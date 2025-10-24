Push-Location $PSScriptRoot
Push-Location ../..

try {
    deactivate
} catch {
}
& "./definition/runner/.venv/scripts/activate.ps1"
pip freeze > ./definition/runner/requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/definition_runner/output"
$exclude_folders = @(".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = $publish_folder + "/shared"
$publish_stepdefinitions_folder = $publish_folder + "/stepdefinitions"
$publish_stephandlers_folder = $publish_folder + "/stephandlers"
$publish_infrastructure_folder = $publish_folder + "/infrastructure"

python $copy_folder_script ./definition/runner $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./definition/shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./stepdefinitions $publish_stepdefinitions_folder -e $exclude_folders
$step_handlers_with_definitions = @("sendtoviberchannel", "getcontentfromjson")
foreach ($step_child_folder in $step_handlers_with_definitions) {
    New-Item -Path $publish_stephandlers_folder/$step_child_folder -ItemType Directory
    Copy-Item -Path ./stephandlers/$step_child_folder/definition.py -Destination $publish_stephandlers_folder/$step_child_folder
}
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
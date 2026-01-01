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
$publish_infrastructure_folder = $publish_folder + "/infrastructure"

python $copy_folder_script ./definition/webapi $publish_folder -e $exclude_folders
python $copy_folder_script ./shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./definition/shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./infrastructure $publish_infrastructure_folder -e $exclude_folders
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder
python $copy_folder_script ./../html_sources $publish_folder/html_sources

Pop-Location
Pop-Location
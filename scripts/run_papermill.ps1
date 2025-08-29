param(
    [Parameter(Mandatory=$true)][string]$InputGcs,
    [Parameter(Mandatory=$true)][string]$OutputIpynb,
    [string]$ParametersJson='{}'
)
$gcloud = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
$venvPy = "~/.venvs/formauto/bin/python"
$nb = "~/project/notebooks/問い合わせURL取得.ipynb"
& $gcloud compute ssh form-sales-machine --zone asia-northeast1-b --command "$venvPy -m papermill $nb $OutputIpynb -p input_gcs $InputGcs"

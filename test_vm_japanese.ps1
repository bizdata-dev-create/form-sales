# PowerShell script to test Japanese text on VM
$Instance = "form-sales-machine"
$Zone = "asia-northeast1-b"

# Set encoding
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding = [System.Text.Encoding]::UTF8

# Test 1: Simple echo command
Write-Host "=== テスト1: シンプルなechoコマンド ==="
gcloud compute ssh $Instance --zone $Zone --command "echo '営業文生成テスト: 日本語文字化けなし'"

# Test 2: Python script execution
Write-Host "`n=== テスト2: Pythonスクリプト実行 ==="
gcloud compute ssh $Instance --zone $Zone --command "cd ~/form-sales && source venv/bin/activate && python3 -c 'print(\"営業文生成テスト: 日本語文字化けなし\")'"

# Test 3: File creation and reading
Write-Host "`n=== テスト3: ファイル作成と読み取り ==="
gcloud compute ssh $Instance --zone $Zone --command "cd ~/form-sales && echo '営業文生成テスト' > japanese_test.txt && cat japanese_test.txt"

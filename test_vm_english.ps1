# PowerShell script to test VM functionality without Japanese characters
$Instance = "form-sales-machine"
$Zone = "asia-northeast1-b"

# Set encoding
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding = [System.Text.Encoding]::UTF8

Write-Host "=== Test 1: Simple echo command ==="
gcloud compute ssh $Instance --zone $Zone --command "echo 'Sales copy generation test: No garbled characters'"

Write-Host "`n=== Test 2: Python script execution ==="
gcloud compute ssh $Instance --zone $Zone --command "cd ~/form-sales && source venv/bin/activate && python3 -c 'print(\"Sales copy generation test: No garbled characters\")'"

Write-Host "`n=== Test 3: File creation and reading ==="
gcloud compute ssh $Instance --zone $Zone --command "cd ~/form-sales && echo 'Sales copy generation test' > english_test.txt && cat english_test.txt"

Write-Host "`n=== Test 4: Locale check ==="
gcloud compute ssh $Instance --zone $Zone --command "locale | grep LANG"

Write-Host "`n=== Test 5: Python encoding check ==="
gcloud compute ssh $Instance --zone $Zone --command "cd ~/form-sales && source venv/bin/activate && python3 -c 'import sys; print(\"Python encoding: \" + sys.getdefaultencoding()); print(\"stdout encoding: \" + sys.stdout.encoding)'"

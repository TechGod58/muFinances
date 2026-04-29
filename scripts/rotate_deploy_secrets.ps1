param(
  [string]$SecretDirectory = "deploy/secrets"
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$secretPath = Join-Path $root $SecretDirectory
New-Item -ItemType Directory -Force -Path $secretPath | Out-Null

function New-UrlSafeSecret {
  param([int]$Bytes = 48)
  $buffer = [byte[]]::new($Bytes)
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($buffer)
  return [Convert]::ToBase64String($buffer).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

$fieldKeyPath = Join-Path $secretPath "mufinances_field_key.txt"
$postgresPasswordPath = Join-Path $secretPath "postgres_password.txt"

Set-Content -LiteralPath $fieldKeyPath -Value (New-UrlSafeSecret -Bytes 48) -Encoding ascii -NoNewline
Set-Content -LiteralPath $postgresPasswordPath -Value (New-UrlSafeSecret -Bytes 36) -Encoding ascii -NoNewline

Write-Host "Rotated local deploy secrets:"
Write-Host " - $fieldKeyPath"
Write-Host " - $postgresPasswordPath"
Write-Host "These files are ignored by git and must be copied to the target server through the approved secret channel."

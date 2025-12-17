param(
  [int]$Offset = 1500,
  [string]$RpcHost = "127.0.0.1",
  [int]$Port = 7362
)

# Build the XML-RPC endpoint URI safely (avoid reserved $Host var)
$uri = ("http://{0}:{1}/RPC2" -f $RpcHost, $Port)

$body = @"
<?xml version="1.0"?>
<methodCall>
  <methodName>fldigi.main.shell</methodName>
  <params>
    <param><value><string>FLDIGI.WFHZ:$Offset</string></value></param>
  </params>
</methodCall>
"@

try {
  $resp = Invoke-WebRequest -Uri $uri -Method Post -Body $body -ContentType "text/xml" -TimeoutSec 5
  Write-Host "Posted FLDIGI.WFHZ:$Offset to $uri; HTTP $($resp.StatusCode)"
  Write-Host "Response:`n$($resp.Content)"
} catch {
  Write-Host "Request failed: $($_.Exception.Message)"
}

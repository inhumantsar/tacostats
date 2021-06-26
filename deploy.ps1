aws --region us-west-2 ecr get-login-password | docker login --username AWS --password-stdin 676444348764.dkr.ecr.us-west-2.amazonaws.com
$version = $(Get-Content VERSION)
docker build -t 676444348764.dkr.ecr.us-west-2.amazonaws.com/tacostats:v$version -f .\lambda.Dockerfile .
docker push 676444348764.dkr.ecr.us-west-2.amazonaws.com/tacostats:v$version
& 'C:\Program Files\Amazon\AWSSAMCLI\bin\sam.cmd' deploy --config-file .\samconfig.toml -t .\lambda.template.yaml
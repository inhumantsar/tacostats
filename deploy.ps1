aws --region us-east-2 ecr get-login-password | docker login --username AWS --password-stdin 390721581096.dkr.ecr.us-east-2.amazonaws.com
$version = $(Get-Content VERSION)
docker build -t 390721581096.dkr.ecr.us-east-2.amazonaws.com/tacostats:v$version -f .\lambda.Dockerfile .
docker push 390721581096.dkr.ecr.us-east-2.amazonaws.com/tacostats:v$version
& 'C:\Program Files\Amazon\AWSSAMCLI\bin\sam.cmd' deploy --config-file .\samconfig.toml -t .\lambda.template.yaml
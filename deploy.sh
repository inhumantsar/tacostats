aws --region us-east-2 ecr get-login-password | sudo docker login --username AWS --password-stdin 390721581096.dkr.ecr.us-east-2.amazonaws.com
VERSION=$(cat VERSION)
sudo docker build -t 390721581096.dkr.ecr.us-east-2.amazonaws.com/tacostats:v$VERSION -f lambda.Dockerfile .
sudo docker push 390721581096.dkr.ecr.us-east-2.amazonaws.com/tacostats:v$VERSION
sam deploy --config-file samconfig.toml -t lambda.template.yaml
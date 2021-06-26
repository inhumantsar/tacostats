# tacostats

Periodically grab comments from the r/neoliberal DT and gather some basic statistics

## Requirements

* Python 3.8+
* MySQL Development libraries
* GCC and friends

## Development

This was developed on Windows and designed to run on Amazon Linux. It should be completely cross-platform.

    yum install -y make glibc-devel gcc patch mysql-devel
    pip install wheel nltk  # installing these first will make the next install go faster
    pip install -r requirements.txt

### Run a Lambda Function Locally

    docker build -t 676444348764.dkr.ecr.us-west-2.amazonaws.com/tacostats -f .\lambda.Dockerfile .   
    & 'C:\Program Files\Amazon\AWSSAMCLI\bin\sam.cmd' local invoke -t .\lambda.template.yaml StatsRecap


### Deploy Lambda Functions

    bump2version [major|minor|patch]
    .\deploy.ps1
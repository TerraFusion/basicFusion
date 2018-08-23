FROM terrafusion/basicfusion

#################
## AWS WORKFLOW #
#################

RUN apt-get -y install python-pip python-dev build-essential && pip install --upgrade pip

RUN pip install awscli --upgrade \
	&& pip install bfutils

ADD ./75000_s3 /bf/infilelist/75000

ADD ./awsrun.py /bf/awsrun.py

#ADD ./.aws /root/.aws

ADD ./Dockerfile /

#ENTRYPOINT ["/usr/local/bin/basicfusion"]
#CMD ["/usr/local/bin/basicfusion"]

FROM debian

WORKDIR /bf

ADD ./hdf-4.2.13 /bf/hdf-4.2.13
ADD ./hdf5-1.8.16 /bf/hdf5-1.8.16
ADD ./basicFusion /bf/basicFusion

RUN apt-get update \
	&& apt-get install -y apt-utils \
	&& apt-get -y install libpng-dev \
	&& apt-get -y install zlib1g-dev  \
	&& apt-get -y install make \
	&& apt-get -y install gcc \
	&& apt-get -y install bison flex \
	&& apt-get -y install libjpeg-dev \
	&& apt-get -y install g++ \
	&& apt-get -y install libncurses-dev

RUN cd /bf/hdf-4.2.13 && ./configure --prefix=/usr/local --enable-shared --disable-fortran && \
	make && \
	make install

RUN cd /bf/hdf5-1.8.16 && ./configure --prefix=/usr/local --disable-fortran && \
	make && \
	make install

RUN cd /bf/basicFusion && \
	make

#ENTRYPOINT ["/usr/local/bin/basicfusion"]

CMD ["/usr/local/bin/basicfusion"]


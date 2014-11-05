NAME               = rabbitmq-server
PKGROOT            = /opt/rabbitmq-server
VERSION            = 3.3.4
RELEASE            = 1

SRC_SUBDIR         = rabbitmq-server

SOURCE_NAME        = $(NAME)
SOURCE_VERSION     = $(VERSION)
SOURCE_SUFFIX      = tar.gz
SOURCE_PKG         = $(SOURCE_NAME)-$(SOURCE_VERSION).$(SOURCE_SUFFIX)
SOURCE_DIR         = $(SOURCE_PKG:%.$(SOURCE_SUFFIX)=%)

TAR_GZ_PKGS           = $(SOURCE_PKG)

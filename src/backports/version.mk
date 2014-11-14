PKGROOT		   = /opt/backports.ssl_match_hostname
NAME               = backports.ssl_match_hostname
VERSION            = 3.4.0.2
RELEASE            = 1
TARBALL_POSTFIX    = tar.gz

SRC_SUBDIR         = backports.ssl_match_hostname

SOURCE_NAME        = $(NAME)
SOURCE_VERSION     = $(VERSION)
SOURCE_SUFFIX      = tar.gz
SOURCE_PKG         = $(SOURCE_NAME)-$(SOURCE_VERSION).$(SOURCE_SUFFIX)
SOURCE_DIR         = $(SOURCE_PKG:%.$(SOURCE_SUFFIX)=%)

TAR_GZ_PKGS           = $(SOURCE_PKG)

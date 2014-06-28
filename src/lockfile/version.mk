PKGROOT		   = usr
NAME               = lockfile
VERSION            = 0.8
RELEASE            = 1
TARBALL_POSTFIX    = tar.gz

SRC_SUBDIR         = python-daemon

SOURCE_NAME        = $(NAME)
SOURCE_VERSION     = $(VERSION)
SOURCE_SUFFIX      = tar.gz
SOURCE_PKG         = $(SOURCE_NAME)-$(SOURCE_VERSION).$(SOURCE_SUFFIX)
SOURCE_DIR         = $(SOURCE_PKG:%.$(SOURCE_SUFFIX)=%)

TAR_GZ_PKGS           = $(SOURCE_PKG)


PORTNAME=	mcp
PORTVERSION=	0.1
CATEGORIES=	net
MASTER_SITES=	http://nonexistinghost.rambler.ru/

MAINTAINER=	d.zatsepin@rambler-co.ru
COMMENT=	Network multi-destination file copy tool.

USE_BZIP2=	yes

BUILD_DEPENDS=  cmake:${PORTSDIR}/devel/cmake

do-configure:
	cd ${WRKSRC} && ${LOCALBASE}/bin/cmake .

.include <bsd.port.mk>

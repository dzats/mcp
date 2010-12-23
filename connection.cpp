#include <sys/socket.h>
#include <sys/time.h>
#include <net/if.h>
#include <sys/ioctl.h>

#include "connection.h"

using namespace std;

// Internal function to inmplement a more precise usleep on FreeBSD
void internal_usleep(unsigned udelay) {
  // FIXME: Check whether this method is appropriate
#ifndef linux
  if (udelay < 1000) {
    struct timeval current_time;
    struct timeval till_time;
    gettimeofday(&till_time, NULL);
    till_time.tv_usec += udelay;
    till_time.tv_sec += till_time.tv_usec / 1000000;
    till_time.tv_usec = till_time.tv_usec % 1000000;
    do {
      if (udelay > 66) {
        sched_yield();
      }
      gettimeofday(&current_time, NULL);
    } while (current_time.tv_sec < till_time.tv_sec ||
      current_time.tv_sec == till_time.tv_sec &&
      current_time.tv_usec < till_time.tv_usec);
  } else {
    usleep(udelay);
  }
#else
  usleep(udelay);
#endif
}

// Receive 'size' bytes from 'sock' and places them to 'data'
void recvn(int sock, void *data, size_t size)
{
  do {
    register int bytes_recvd = recv(sock, data, size, 0);
    if (bytes_recvd <= 0) {
      throw ConnectionException(errno);
    } else {
      size -= bytes_recvd;
      data = (uint8_t *)data + bytes_recvd;
    }
  } while(size > 0);
}

// Send 'size' bytes from 'data' to 'sock'
void sendn(int sock, const void *data, size_t size, int flags)
{
  do {
    register int bytes_sent = send(sock, data, size, flags);
    if (bytes_sent < 0) {
      if (errno == ENOBUFS) {
        SDEBUG("ENOBUFS error occurred\n");
        continue;
      }

      throw ConnectionException(errno);
    } else {
      size -= bytes_sent;
      data = (uint8_t *)data + bytes_sent;
    }
  } while(size > 0);
}

void send_normal_conformation(int sock, uint32_t addr)
{
  ReplyHeader rh(STATUS_OK, addr, 0);
  sendn(sock, &rh, sizeof(rh), 0);
}

void send_incorrect_checksum(int sock, uint32_t addr)
{
  ReplyHeader rh(STATUS_INCORRECT_CHECKSUM, addr, 0);
  sendn(sock, &rh, sizeof(rh), 0);
}

void send_server_is_busy(int sock, uint32_t addr)
{
  ReplyHeader rh(STATUS_SERVER_IS_BUSY, addr, 0);
  sendn(sock, &rh, sizeof(rh), 0);
}

// Receives reply from 'sock'. Returns 0 if some reply has been received
// and -1 otherwise
int ReplyHeader::recv_reply(int sock, char **message, int flags)
{
  register int recv_result;
  recv_result = recv(sock, this, sizeof(ReplyHeader), flags);
  if (recv_result > 0) {
    // Something received
    if ((unsigned)recv_result < sizeof(ReplyHeader)) {
      // Receive remaining part of the header
      recvn(sock, (uint8_t *)this + recv_result,
        sizeof(ReplyHeader) - recv_result);
    }
    if (get_msg_length() > MAX_ERROR_LENGTH) {
      throw ConnectionException(ConnectionException::corrupted_data_received);
    } else if (get_msg_length() > 0) {
      *message = (char *)malloc(get_msg_length() + 1);
      (*message)[get_msg_length()] = '\0';
      recvn(sock, *message, get_msg_length());
      DEBUG("Received error message: %u, %x, %u, %s\n", get_status(),
        get_address(), get_msg_length(), *message);
    }
    return 0;
  } else {
    if ((flags & MSG_DONTWAIT) == 0 || errno != EAGAIN) {
      throw ConnectionException(errno);
    }
    return -1;
  }
}

// Returns internet addresses, which the host has
// The returned value should be futher explicitly deleted.
int get_local_addresses(int sock, vector<uint32_t> *addresses,
    vector<uint32_t> *masks) {
  struct ifconf ifc;

  // Get the available interfaces
  int lastlen = 0;
  ifc.ifc_len = sizeof(struct ifreq) * 28;
  ifc.ifc_req = (struct ifreq *)malloc(ifc.ifc_len);
  while(1) {
    if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
      ERROR("ioctl(SIOCGIFCONF) call returned the error: %s", strerror(errno));
      return -1;
    }
    if (ifc.ifc_len != lastlen || lastlen == 0) {
      lastlen = ifc.ifc_len;
      ifc.ifc_len += sizeof(struct ifreq) * 12;
      ifc.ifc_req = (struct ifreq *)realloc(ifc.ifc_req, ifc.ifc_len);
    } else {
      break;
    }
  }

  DEBUG("Number of interfaces: %d(%zu)\n", ifc.ifc_len, sizeof(struct ifreq));
  for (uint8_t *ptr = (uint8_t *)ifc.ifc_req;
      ptr < (uint8_t *)ifc.ifc_req + ifc.ifc_len;) {
    struct ifreq *ifr = (struct ifreq *)ptr;

#ifdef _SIZEOF_ADDR_IFREQ
    /*
      ptr += sizeof(ifr->ifr_name) +
        max(sizeof(struct sockaddr), (unsigned)ifr->ifr_addr.sa_len);
    */
    ptr += _SIZEOF_ADDR_IFREQ((*ifr));
#else
    // For linux compartability
    ptr = (uint8_t *)(ifr + 1);
#endif

    if (ifr->ifr_addr.sa_family == AF_INET) {
      uint32_t addr = ((struct sockaddr_in *)&ifr->ifr_addr)->sin_addr.s_addr;

      // Get flags for the interface
      if (ioctl(sock, SIOCGIFFLAGS, ifr) < 0) {
        ERROR("ioctl(SIOCGIFFLAGS) call returned the error: %s",
          strerror(errno));
        return -1;
      }
      if ((ifr->ifr_flags & IFF_UP) == 0 ||
          (ifr->ifr_flags & IFF_LOOPBACK) != 0) {
        continue;
      }

#ifndef NDEBUG
      char iaddr[INET_ADDRSTRLEN];
      DEBUG("Interface %s, address %s\n", ifr->ifr_name, 
        inet_ntop(AF_INET, &addr, iaddr, sizeof(iaddr)));
#endif
      addresses->push_back(ntohl(addr));
      if (masks != NULL) {
        // Get network mask for the interface
        if (ioctl(sock, SIOCGIFNETMASK, ifr) < 0) {
          ERROR("Can't get network mask for the interface %s: %s",
            ifr->ifr_name, strerror(errno));
          return -1;
        }
        uint32_t addr = ((struct sockaddr_in *)&ifr->ifr_addr)->sin_addr.s_addr;
        DEBUG("Network mask: %s\n", 
          inet_ntop(AF_INET, &addr, iaddr, sizeof(iaddr)));
        masks->push_back(ntohl(addr));
      }
    }
  }
  free(ifc.ifc_req);
  return 0;
}

#ifndef LOG_H
#define LOG_H 1
#include <syslog.h>

// This solution is temporary. The actual logging should use syslog
#define QUOTIZE(x) #x
#define NUMBER_TO_STRING(x) QUOTIZE(x)
#ifndef NDEBUG
#define DEBUG(message, ...) \
  printf("%5u(" __FILE__ ":" NUMBER_TO_STRING(__LINE__) ") " message, \
    getpid(), __VA_ARGS__)
#define SDEBUG(message) \
  printf("%5u(" __FILE__ ":" NUMBER_TO_STRING(__LINE__) ") " message, \
    getpid())
#else
#define DEBUG(message, ...) ((void)0)
#define SDEBUG(message) ((void)0)
#endif

#ifdef NDEBUG
#define ERROR(message, ...) \
  syslog(LOG_ERR, "(" __FILE__ ":" NUMBER_TO_STRING(__LINE__) ") " \
    message, __VA_ARGS__)
#define SERROR(message) \
  syslog(LOG_ERR, "(" __FILE__ ":" NUMBER_TO_STRING(__LINE__) ") " \
    message)
#else
#define ERROR(message, ...) \
  fprintf(stderr, "%5u(" __FILE__ ":" NUMBER_TO_STRING(__LINE__) ") " \
    message, getpid(), __VA_ARGS__)
#define SERROR(message) \
  fprintf(stderr, "%5u(" __FILE__ ":" NUMBER_TO_STRING(__LINE__) ") " \
    message, getpid())
#endif
#endif


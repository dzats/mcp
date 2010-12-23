#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h> // for MAXPATHLEN
#include <poll.h> // for poll

#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>

#include <stack>

#include "reader.h"
#include "file_writer.h" // for FileWriter::get_targetfile_name
#include "log.h"

using namespace std;

// Register an error and finish the current task
void Reader::register_error(uint8_t status, const char *fmt, ...)
{
  va_list args;
  va_start(args, fmt);
  char *error_message = (char *)malloc(MAX_ERROR_MESSAGE_SIZE);
  vsnprintf(error_message, MAX_ERROR_MESSAGE_SIZE, fmt, args);
  errors.add(new SimpleError(status, INADDR_NONE, error_message,
    strlen(error_message)));
  reader.status = status;
  if (status >= STATUS_FIRST_FATAL_ERROR) {
    // Finish the reader
    finish_task();
    finish_work();
  }
  va_end(args);
}

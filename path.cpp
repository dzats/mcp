#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <assert.h>

#include "path.h"
#include "log.h"

// Check for MAXPATHLEN required

// Returns the name of the target file
const char* get_targetfile_name(const char *source_name,
    const char *path, PathType path_type)
{
  // Figure out the file name
  if (path_type == path_is_default_directory) {
    return source_name;
  } else if (path_type == path_is_directory) {
    char *filename = (char *)malloc(strlen(path) + 1 /* slash */ +
      strlen(source_name) + 1);
    sprintf(filename, "%s/%s", path, source_name);
    return filename;
  } else if (path_type == path_is_regular_file ||
      path_type == path_is_nonexisting_object) {
    return path;
  } else {
    assert(path_type == path_is_substituted_directory);
    int pathlen = strlen(path);
    char *filename = (char *)malloc(pathlen + 1 /* slash */ +
      strlen(source_name) + 1);
    memcpy(filename, path, pathlen);
    char *slash = strchr(source_name, '/');
    assert(slash != NULL);
    strcpy(filename + pathlen, slash);
    return filename;
  }
}

// Release the name returned by the get_targetfile_name function
void free_targetfile_name(const char *filename,
    PathType path_type)
{
  if (path_type == path_is_directory ||
      path_type == path_is_substituted_directory) {
    free(const_cast<char *>(filename));
  }
}

// Returns the name of the target directory. path_type
// is a value-result argument
const char* get_targetdir_name(const char *source_name,
    const char *path, PathType *path_type)
{
  // Figure out the directory name
  if (*path_type == path_is_default_directory) {
    return source_name;
  } else if (*path_type == path_is_directory) {
    char *dirname = (char *)malloc(strlen(path) + 1 /* slash */ +
      strlen(source_name) + 1);
    sprintf(dirname, "%s/%s", path, source_name);
    return dirname;
  } else if (*path_type == path_is_regular_file) {
    return NULL;
  } else if (*path_type == path_is_nonexisting_object) {
    *path_type = path_is_substituted_directory;
    // strdup is required for subsequent free_targetfile_name
    return strdup(path);
  } else {
    assert(*path_type == path_is_substituted_directory);
    int pathlen = strlen(path);
    char *dirname = (char *)malloc(pathlen + 1 /* slash */ +
      strlen(source_name) + 1);
    memcpy(dirname, path, pathlen);
    char *slash = strchr(source_name, '/');
    assert(slash != NULL);
    strcpy(dirname + pathlen, slash);
    return dirname;
  }
}

// Release the name returned by the get_targetdir_name function
void free_targetdir_name(const char *dirname,
    PathType path_type)
{
  if (path_type == path_is_directory ||
      path_type == path_is_substituted_directory) {
    free(const_cast<char *>(dirname));
  }
}

// Detect the type of the 'path'. If this function returns path_is_invalid
// 'error' points to the corresponding error message
PathType get_path_type(const char *path, char **error, unsigned n_sources)
{
  assert(error != NULL);
  size_t path_len = strlen(path);
  if (path_len == 0) {
    // The path is not specified
    return path_is_default_directory;
  } else {
    struct stat s;
    int stat_result = stat(path, &s);
    if (stat_result == 0) {
      DEBUG("Path %s exists\n", path);
      if (S_ISDIR(s.st_mode)) {
        return path_is_directory;
      } else if (S_ISREG(s.st_mode)) {
        if (n_sources > 1) {
          *error = strdup("Multiple sources specified, the path can't be a "
            "file");
          return path_is_invalid;
        }
        return path_is_regular_file;
      } else {
        const char *error_format = "Incorrect path %s";
        *error = (char *)malloc(sizeof(error_format) + strlen(path));
        sprintf(*error, error_format, path);
        return path_is_invalid;
      }
    } else if(errno == ENOENT) {
      if (n_sources > 1) {
        // Create the directory with the name 'path'
        if (mkdir(path, S_IRWXU | S_IRGRP | S_IXGRP |
            S_IROTH | S_IXOTH) != 0) {
          const char *error_format = "Can't create directory %s: %s";
          *error = (char *)malloc(sizeof(error_format) + strlen(path) +
            strlen(strerror(errno)));
          sprintf(*error, error_format, path, strerror(errno));
          return path_is_invalid;
        }
        return path_is_directory;
      } else {
        return path_is_nonexisting_object;
      }
    } else {
      const char *error_format = "Incorrect path %s (%s)";
      *error = (char *)malloc(sizeof(error_format) + strlen(path) +
        strlen(strerror(errno)));
      sprintf(*error, error_format, path, strerror(errno));
      return path_is_invalid;
    }
  }
}

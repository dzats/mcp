#ifndef PATH_H_HEADER
#define PATH_H_HEADER 1

// namespace that defines some operation to figure out
// resulting names of files and directories for the destinations
enum PathType {path_is_default_directory,
  path_is_directory,
  path_is_substituted_directory, // for example mcp /etc ...:1
  path_is_regular_file,
  path_is_nonexisting_object,
  path_is_invalid};

// Returns the name of the target file
const char *get_targetfile_name(const char *source_name,
  const char *path, PathType path_type);

// Release the name returned by the get_targetfile_name function
void free_targetfile_name(const char *filename,
  PathType path_type);

// Returns the name of the target directory. path_type
// is a value-result argument
const char *get_targetdir_name(const char *source_name,
  const char *path, PathType *path_type);

// Release the name returned by the get_targetdir_name function
void free_targetdir_name(const char *dirname,
  PathType path_type);

// Detect the type of the 'path'. If this function returns path_is_invalid
// 'error' points to the corresponding error message
PathType get_path_type(const char *path, char **error, unsigned n_sources);
#endif

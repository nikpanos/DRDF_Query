package gr.unipi.datacron.common

import gr.unipi.datacron.common.Consts.{qfpHdfsPrefix, qfpNamenode}

object Utils {
  def resolveHdfsPath(directory: String) = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpNamenode) + AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(directory)
  }
  else {
    AppConfig.getString(directory)
  }

  def sanitize(input: String): String = s"`$input`"
}

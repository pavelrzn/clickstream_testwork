package com.clickstream

object Сonstants {
  val logDir = "./cl_stream_logs/"
  val catalog = Seq(
    "aaa_br1" -> "pantalonni",
    "bbb_br2" -> "nike",
    "ccc_br3" -> "puma",
    "bbb_br4" -> "Yudashkin",
    "ddd_br5" -> "YetDress",
    "eee_br1" -> "pantalonni",
    "fff_br1" -> "pantalonni",
    "ggg_br1" -> "pantalonni",
    "eee_br2" -> "nike",
    "eee_br2" -> "nike",
    "eee_br4" -> "Yudashkin",
    "ccc_br4" -> "Yudashkin",
    "aaa_br5" -> "YetDress",
    "hhh_br3" -> "puma",
    "iii_br6" -> "DG"
  )

  val dateFormat = "ddMMyyyy"

  val GOOD_ID_COL = "good_id"
  val BRAND_ID_COL = "brand_id"
  val USER_COL = "user"
}

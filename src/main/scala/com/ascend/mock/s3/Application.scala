package com.ascend.mock.s3

import com.ascend.mock.s3.config.Parameters
import com.ascend.mock.s3.model.Books.{bookPFFilter, bookPFPositive, bookScore}
import com.ascend.mock.s3.model.ParametersModel
import com.ascend.mock.s3.spark.S3Read.{getBook, writeBook}
import com.ascend.mock.s3.spark.SparkSessionWrapper

object Application extends App with SparkSessionWrapper {
  val params: ParametersModel = Parameters.parse(args)
  if (!params.flagScore) {
    val df = getBook(params.hashOrigem)
    val pfFilter = bookPFFilter(df)
    writeBook(pfFilter, params.odinPfFilter)
    val pfPositive = bookPFPositive(df)
    writeBook(pfPositive, params.odinPfPositive)
  } else if (params.flagScore) {
    val df = getBook(params.hashOrigem)
    val score = bookScore(df, params.score, params.start, params.end)
    writeBook(score, params.bookScore)
  }
}

package com.hcx.session

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
 * @Author HCX
 * @Description //TODO 需求一：根据session，将用户访问数据统计并插入到mysql数据库中
 * @Date 15:52 2019-11-06
 *
 * @return
 * @exception
 **/

object SessionStat {

  def getSessionFullInfo(sparkSession: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = session2GroupActionRDD.map{
      case (sessionId,iterableAction) =>
        var userId = -1L
        var startTime:Date = null
        var endTime : Date = null
        var stepLength = 0

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")
        for(action <- iterableAction){
          if (userId == -1L)
            userId = action.user_id
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)){
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)){
            endTime = actionTime
          }
          val searchKeyword = action.search_keyword
          if(StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)){
            searchKeywords.append(searchKeyword + ",")
          }
          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)){
            clickCategories.append(clickCategoryId + ",")
          }
          stepLength += 1
        }
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        val aggrInfo  = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId,aggrInfo)
    }
    import sparkSession.implicits._
    val sql  = "select * from user_info"
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id,item))

    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map{
      case (userId,(aggrInfo,userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_SESSION_ID)
        (sessionId,fullInfo)
    }
    sessionId2FullInfoRDD
  }

    /**
   * @Author HCX
   * @Description //TODO 累加器累加visitLength
   * @Date 14:28 2019-11-06
   * [visitLength, sessionStatisticAccumulator]
   * @return Unit
   * @exception
   **/
  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3){
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if(visitLength >=4 && visitLength  <= 6){
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  /**
   * @Author HCX
   * @Description //TODO 累加器累加steplength
   * @Date 14:25 2019-11-06
   * [stepLength, sessionStatisticAccumulator]
   * @return Unit
   * @exception
   **/
  def calcuteStepLength(stepLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if(stepLength >=1 && stepLength <=3){
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilterRDD(taskParam: JSONObject, userId2AggrInfoRDD: RDD[(String, String)], sessionAccumulator:SessionAccumulator) = {
    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "")+
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0,filterInfo.length - 1)
    userId2AggrInfoRDD.filter{
      case(sessionid,fullInfo) =>
        var success = true
        if (!ValidUtils.between(fullInfo,Constants.FIELD_AGE,filterInfo,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
          success = false
        }else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)){
          success = false
        }else if(!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)){
          success = false
        }
        if(success){
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength,sessionAccumulator)
          calcuteStepLength(stepLength,sessionAccumulator)
        }
        success
    }

  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio= NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID,session_count.toInt,visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write.format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","session_stat_ratio_0802")
      .mode(SaveMode.Append)
      .save()
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","贺楚翔")
    //获取筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //获取筛选条件对应的jsonobject
    val taskParam = JSONObject.fromObject(jsonStr)
    //创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取原始的动作表数据  actionRDD : RDD[UserVisitAction]
    val actionRDD = getOriActionRDD(sparkSession,taskParam)
    // sessionId2ActionRDD  : RDD[(session_id,UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id,item))
    // session2GroupActionRDD : RDD[(session_id,iterator(UserVisitAction))]
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    //使用默认存储级别（Memory_only）保留rdd
    session2GroupActionRDD.cache()
    //转换为userId2AggrInfoRDD（sessionid,该用户浏览的记录综合）
    val userId2AggrInfoRDD = getSessionFullInfo(sparkSession,session2GroupActionRDD)

    //注册累加器
    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    val sessionId2FilterRDD = getSessionFilterRDD(taskParam,userId2AggrInfoRDD,sessionAccumulator)

    sessionId2FilterRDD.foreach(println(_))

    //计算结果，并将结果数据插入到mysql数据库中
    getSessionRatio(sparkSession,taskUUID,sessionAccumulator.value)
  }
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    import sparkSession.implicits._
    val sql = "select * from user_visit_action where date >='" + startDate + "' and date<='" + endDate + "'"
    val sql1 = "select * from user_visit_action"
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}

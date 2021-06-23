package com.oldtan.his.domain

import java.time.LocalDateTime

import com.oldtan.his.domain.RegisterStatus.RegisterStatus

/**
  * 就诊挂号 Root Domain
  * @param p 患者
  * @param d 医生
  * @param de 就诊科室
  * @param o 挂号操作员
  */
class RegisterDO(var p: Patient, var d: Doctor, var de: Department, var o: Operator) {

  val id = {
     //TODO 产生挂号流水号
  }

  val operatorTime = LocalDateTime.now //操作时间

  var status = RegisterStatus.create   //挂号状态

  var payItems:List[PayItem] = null           //缴费明细

  /**
    * 新增患者挂号订单
    * registerFun函数新增逻辑暴露给业务层
    * 领域强制校验
    * */
  def register(registerFun: RegisterDO => RegisterDO): RegisterDO = {
    val newRegisterDO = registerFun(this)
    //TODO 领域层通用操作
    //    1、持久化前操作
    //    2、持久化 newRegisterDO
    //    3、持久化后操作
    newRegisterDO
  }

  /**
    * 修改挂号订单
    * modifyFun函数修改逻辑暴露给业务层
    * 领域强制校验：对修改后产生的RegisterDO在此只做流水号、患者id不能被修改的校验
    **/
  def modify(modifyFun: RegisterDO => RegisterDO): RegisterDO = {
    val newRegisterDO = modifyFun(this)
    //TODO 领域层通用操作
    //    1、持久化前操作
    //    2、持久化 newRegisterDO
    //    3、持久化后操作
    newRegisterDO
  }

  /**
    * 撤销挂号
    * cancelFun函数撤销逻辑暴露给业务层，返回是否可以撤销挂号boolean value, false->可撤销  true->不可撤销
    */
  def cancel(cancelFun: RegisterDO => Boolean) = {
    assert(!(cancelFun(this)), "您当前不能进行挂号撤销操作!")
    assert(status == RegisterStatus.cancel, "就诊中状态不可撤单")
    assert(status == RegisterStatus.cancel, "本挂号单已是撤单状态")
    //TODO 领域层通用操作
    //    1、持久化前操作
    //    2、持久化操作逻辑撤销
    //    3、持久化后操作
  }

  /**
    * 支付明细
    * payItemFun函数逻辑暴露给业务层,可以自定义
    */
  def readPayItem(payItemFun: RegisterDO => List[PayItem]): RegisterDO ={
    this.payItems = payItemFun(this)
    //TODO 领域层通用操作
    //   1、持久化支付明细
    this
  }

  /**
    * 完成支付操作
    */
  def pay()={
    status = RegisterStatus.pay
    //TODO 持久层修改支付状态
  }

}

object RegisterDO{

  /**
    * 可通过不同参数进行挂号订单的分页查询
    * @param rStatus 挂号状态
    * @param s 患者
    * @param de 科室
    * @param d 医生
    * @return
    */
  def query(rStatus:RegisterStatus=null, s:Patient=null, de:Department=null, d:Doctor=null) : List[RegisterDO] ={
    //TODO 持久层组装条件分页查询
    null
  }

  /**
    * 单个查询
    * @param rId
    * @return
    */
  def query(rId:String):RegisterDO={
    //TODO 持久层单个查询
    null
  }

}

case class Patient(id: String, var name: String, sex: Char, var address: String, var phone: Int)

case class Department(id:String, var name: String)

case class Doctor(id: String, name: String, sex: Char, phone: Int)

case class Operator(var id: String, var name: String)

case class PayItem(payType:String, money: Double)

object RegisterStatus extends Enumeration{
  type RegisterStatus = Value
  val create = Value(1)    //初始化状态
  val pay = Value(2)       //完成缴费
  val seeDocker = Value(3) //就诊中
  val cancel = Value(0)    //撤单
}

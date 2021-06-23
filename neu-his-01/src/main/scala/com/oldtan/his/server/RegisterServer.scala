package com.oldtan.his.server

import com.oldtan.his.api.RegisterDto
import com.oldtan.his.domain.{Department, Doctor, Operator, Patient, PayItem, RegisterDO}

/**
  * 挂号领域业务应用层面，主要代码逻辑包括：
  *  1、可以调用领域层DO，并对领域层暴露的函数进行领域通用接口的个性化业务定制
  *  2、可以组装多个领域，进行单个领域的操作
  */
object RegisterServer {

  /**
    * 挂号新增
    * @param dto
    * @param operatorId 操作员ID
    * @param operatorName 操作员姓名
    * @return
    */
  def register(dto: RegisterDto, operatorId:String, operatorName:String): RegisterDO = {
    val registerDO = new RegisterDO(Patient(dto.patientId, dto.patientName, dto.patientSex, dto.patientAddress, dto.patientPhone),
                  Doctor(dto.doctorId, dto.doctorName, dto.doctorSex, dto.doctorPhone),
                  Department(dto.departmentId, dto.departmentName),
                  Operator(operatorId, operatorName))
    registerDO.register(registerDO => {
      //TODO 个性化业务定制处理挂号注册属性，比如修改缴费明细
      registerDO.payItems = List(PayItem("专家挂号费", 12.00), PayItem("工本费", 2.00))
      registerDO
    })
  }

}

package com.oldtan.his.api

import com.oldtan.config.LazyLog
import com.oldtan.his.domain.RegisterDO
import com.oldtan.his.server.RegisterServer
import org.springframework.web.bind.annotation.{RequestBody, RestController}

import scala.beans.BeanProperty

@RestController
class RegisterRest extends LazyLog {

  /**
    * 新增挂号
    * @param dto
    * @return
    */
  def register(@RequestBody dto: RegisterDto): RegisterDO = {
    //TODO 通过网关层获取当前登录人员信息
    val operatorId = ""
    val operatorName = ""
    //调用挂号领域应用层
    RegisterServer.register(dto, operatorId, operatorName)
  }
}

case class RegisterDto(@BeanProperty patientId: String, @BeanProperty patientName: String, @BeanProperty patientAddress: String, @BeanProperty patientPhone: Int, @BeanProperty patientSex: Char,
                       @BeanProperty departmentId: String, @BeanProperty departmentName: String, @BeanProperty doctorId: String,
                       @BeanProperty doctorName: String, @BeanProperty doctorSex: Char, @BeanProperty doctorPhone: Int)


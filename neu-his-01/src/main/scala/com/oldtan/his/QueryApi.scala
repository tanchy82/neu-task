package com.oldtan.his

import java.time.LocalDateTime

import com.oldtan.config.{LazyEsClient, LazyLog}
import org.springframework.web.bind.annotation._

import scala.beans.BeanProperty

@RestController
class QueryApi extends LazyLog with LazyEsClient{

  @GetMapping(value = Array("/user/{id}"))
  def getUser(@PathVariable(value = "id") id: Long):  User = {
    println(s"=====$id")
    log.info(s"=====$id")
    User(id, "oldtan", LocalDateTime.now)
  }

  @PostMapping(value = Array("/user"))
  def postUser(@RequestBody user: User) = {
    println(user)
  }

}
case class User(@BeanProperty id: Long, @BeanProperty name: String, @BeanProperty date:LocalDateTime)

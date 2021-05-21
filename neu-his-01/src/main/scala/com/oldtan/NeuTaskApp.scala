package com.oldtan

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class NeuTaskApp
object NeuTaskApp extends App {
  SpringApplication.run(classOf[NeuTaskApp], args: _*)
}
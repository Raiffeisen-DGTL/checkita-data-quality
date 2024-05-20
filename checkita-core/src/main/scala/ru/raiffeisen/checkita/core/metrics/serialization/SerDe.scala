package ru.raiffeisen.checkita.core.metrics.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

trait SerDe[T] {
  
  protected def decodeSize(bf: ByteArrayInputStream): Int = {
    val sizeBytes = new Array[Byte](4)
    bf.read(sizeBytes)
    ByteBuffer.wrap(sizeBytes).getInt
  }
  
  protected def encodeSize(bf: ByteArrayOutputStream, size: Int): Unit = 
    bf.write(ByteBuffer.allocate(4).putInt(size).array)
  
  protected def readVarSizeValueBytes(bf: ByteArrayInputStream): Array[Byte] = {
    val valueSize = decodeSize(bf)
    val valueBytes = new Array[Byte](valueSize)
    bf.read(valueBytes)
    valueBytes
  }
  
  protected def writeVarSizeValueBytes(bf: ByteArrayOutputStream, valueBytes: Array[Byte]): Unit = {
    encodeSize(bf, valueBytes.length)
    bf.write(valueBytes)
  }
  
  def serialize(bf: ByteArrayOutputStream, value: T): Unit
  
  def deserialize(bf: ByteArrayInputStream): T
}

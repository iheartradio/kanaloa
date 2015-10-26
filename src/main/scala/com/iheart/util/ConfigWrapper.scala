package com.iheart.util

import scala.reflect.runtime.universe.{ typeOf, TypeTag }
import com.typesafe.config._
import scala.collection.JavaConverters._

object ConfigWrapper {
  implicit class ImplicitConfigWrapper(underlying: Config) extends ConfigWrapper(underlying)
}

/**
 * Wrapper for Typesafe Config that provides a more convenient interface for optional values
 *
 * @constructor wrap Typesafe Config
 * @param underlying the original Config object
 */
class ConfigWrapper(underlying: Config) {
  /** Get value at path, throws exception if path not found or type mismatch */
  def get[T: TypeTag](path: String): T = getWithMode(path, Mode.uncaughtMode)

  /** Get optional value at path, returns None if path not found */
  def getOption[T: TypeTag](path: String): Option[T] = getWithMode(path, Mode.optionMode)

  /** Convenience method for getting a value, with a fallback if path not found */
  def getOrElse[T: TypeTag](path: String, fallback: => T): T = getOption[T](path).getOrElse(fallback)

  protected def getWithMode[T: TypeTag](path: String, mode: Mode): mode.Type[T] = typeOf[T] match {
    case t if t =:= typeOf[Boolean] =>
      getValue(path, mode, _.getBoolean)
    case t if t =:= typeOf[List[Boolean]] =>
      getValue(path, mode, c => c.getBooleanList(_).asScala.map(_ == true).toList)

    case t if t =:= typeOf[Double] =>
      getValue(path, mode, _.getDouble)
    case t if t =:= typeOf[List[Double]] =>
      getValue(path, mode, c => c.getDoubleList(_).asScala.map(_.toDouble).toList)

    case t if t =:= typeOf[Int] =>
      getValue(path, mode, _.getInt)
    case t if t =:= typeOf[List[Int]] =>
      getValue(path, mode, c => c.getIntList(_).asScala.map(_.toInt).toList)

    case t if t =:= typeOf[String] =>
      getValue(path, mode, _.getString)
    case t if t =:= typeOf[List[String]] =>
      getValue(path, mode, c => c.getStringList(_).asScala.toList)

    case t if t =:= typeOf[Config] =>
      getValue(path, mode, _.getConfig)

    case t => throw new MatchError(s"Unsupported type `${t.typeSymbol.fullName}` for ConfigWrapper.get")
  }

  protected def getValue[T](path: String, mode: Mode, f: Config => String => Any): mode.Type[T] =
    mode.wrapException[T, ConfigException.Missing] {
      if (underlying.hasPath(path)) {
        f(underlying)(path).asInstanceOf[T]
      } else {
        throw new ConfigException.Missing(path)
      }
    }
}


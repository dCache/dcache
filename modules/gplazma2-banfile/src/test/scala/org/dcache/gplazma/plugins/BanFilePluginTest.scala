package org.dcache.gplazma.plugins

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scala.collection.JavaConversions._
import scala.io.Source

import java.util.Properties
import java.security.Principal

import org.dcache.gplazma.AuthenticationException
import com.google.common.io.Resources.getResource

class TestPrincipal(name : String) extends Principal {
  override def getName: String = name
  override def equals(other : Any) =
       other.getClass == this.getClass &&
       other.asInstanceOf[TestPrincipal].getName ==  name
}

// used in test "should not ban on equal names in different principal classes"
class TestPrincipalB(name : String) extends TestPrincipal(name)

trait stringSource extends BanFilePlugin {
  var sourceString : String = _
  override def fromSource : Source = Source fromString sourceString
}

@RunWith(classOf[JUnitRunner])
class BanFilePluginTest extends FlatSpec {

  def validProperties = {
    val properties = new Properties()
    properties.put(BanFilePlugin.BAN_FILE, getResource("ban.conf").getPath)
    properties
  }

  "The BanFilePlugin" should "throw an IllegalArgumentException if properties is null" in {
    intercept[IllegalArgumentException] {
      new BanFilePlugin(null)
    }
  }

  it should "throw an IllegalArgumentException if the gplazma.banfile.path property is not set" in {
    intercept[IllegalArgumentException] {
      new BanFilePlugin(new Properties)
    }
  }

  it should "throw an IllegalStateException if the config file does not exist at start time" in {
    val properties = new Properties()
    properties.put(BanFilePlugin.BAN_FILE, "/file/does/not/exist")

    intercept[IllegalStateException] {
      new BanFilePlugin(properties).account(null)
    }
  }

  it should "allow a not banned user" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |ban org.dcache.gplazma.plugins.TestPrincipal:bert
      """.stripMargin
    plugin.account(Set[Principal](new TestPrincipal("ernie")))
  }

  it should "not ban on equal names in different principal classes" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |ban org.dcache.gplazma.plugins.TestPrincipalB:ernie
      """.stripMargin
    plugin.account(Set[Principal](new TestPrincipal("ernie")))
  }

  it should "throw an AuthenticatedException for a banned user" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |ban org.dcache.gplazma.plugins.TestPrincipal:bert
      """.stripMargin
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
  }

  it should "work multiple times" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |ban org.dcache.gplazma.plugins.TestPrincipal:bert
      """.stripMargin
    plugin.account(Set[Principal](new TestPrincipal("ernie")))
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
    plugin.account(Set[Principal](new TestPrincipal("ernie")))
  }

  it should "ban user if any principal is blacklisted" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |ban org.dcache.gplazma.plugins.TestPrincipal:bert
      """.stripMargin
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("ernie"), new TestPrincipal("bert")))
    }
  }

  it should "use configured aliases" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |alias name=org.dcache.gplazma.plugins.TestPrincipal
        |ban name:bert
      """.stripMargin
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
  }

  it should "understand 'standard' principals without alias" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |ban name:bert
      """.stripMargin
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new org.dcache.auth.LoginNamePrincipal("bert")))
    }
  }

  it should "ignore comments" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |# some comment
        |alias name=org.dcache.gplazma.plugins.TestPrincipal
        |ban name:bert
      """.stripMargin
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
  }

  it should "ignore whitespace lines" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |alias name=org.dcache.gplazma.plugins.TestPrincipal
        |
        |ban name:bert
      """.stripMargin
    intercept[AuthenticationException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
  }

  it should "throw an IllegalArgumentException on malformed lines" in {
    val plugin = new BanFilePlugin(validProperties) with stringSource
    plugin.sourceString =
      """
        |this is not a valid line
      """.stripMargin
    intercept[IllegalArgumentException] {
      plugin.account(Set[Principal](new TestPrincipal("bert")))
    }
  }
}

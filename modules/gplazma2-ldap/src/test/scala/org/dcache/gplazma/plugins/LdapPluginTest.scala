package org.dcache.gplazma.plugins

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import java.util
import java.util.Properties
import java.security.Principal

import scala.collection.convert.WrapAsJava.setAsJavaSet

import org.dcache.auth.{GroupNamePrincipal, GidPrincipal, UidPrincipal, UserNamePrincipal}
import org.dcache.gplazma.NoSuchPrincipalException
import org.dcache.auth.attributes.{HomeDirectory, RootDirectory}


/**
 * Tests for the gPlazma LDAP plugin.
 *
 * The tests are all ignored by default because they depend on DESY infrastructure.
 */
@RunWith(classOf[JUnitRunner]) @Ignore
class LdapPluginTest extends FlatSpec with Matchers {

  val pluginProperties = {
    val properties = new Properties
    properties.put(Ldap.LDAP_URL, "ldap://wof-dav.desy.de:389/")
    properties.put(Ldap.LDAP_ORG, "ou=NIS,o=DESY,c=DE")
    properties.put(Ldap.LDAP_USER_FILTER, "(uid=%s)")
    properties.put(Ldap.LDAP_PEOPLE_TREE, "People")
    properties.put(Ldap.LDAP_GROUP_TREE, "Groups")
    properties.put(Ldap.LDAP_USER_HOME, "/root")
    properties.put(Ldap.LDAP_USER_ROOT, "/root%homeDirectory%/home")
    properties.put(Ldap.LDAP_GROUP_MEMBER, "uniqueMember")
    properties
  }

  def ldapPlugin = new Ldap(pluginProperties)

  "map(Set[Principal])" should "return matching Uid and Gid Principals for an existent user name" in {
    val principals = new util.HashSet[Principal]()
    principals add new UserNamePrincipal("testuser")

    ldapPlugin.map(principals)
    principals.size should be (3)
    principals should contain (new UserNamePrincipal("testuser"))
    principals should contain (new UidPrincipal("50999"))
    principals should contain (new GidPrincipal("3752", true))
  }

  it should "leave the principals set unchanged for a non existent user name" in {
    val principals = new util.HashSet[Principal]()
    principals add new UserNamePrincipal("bert")

    ldapPlugin.map(principals)
    principals should have size 1
    principals should contain (new UserNamePrincipal("bert"))
  }

  "map(UserNamePrincipal)" should "return a UidPrincipal for an existing user name" in {
    ldapPlugin.map(new UserNamePrincipal("testuser")) should be (new UidPrincipal("50999"))
  }

  it should "throw a NoSuchPrincipalException if a user does not exist" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.map(new UserNamePrincipal("bert"))
    }
  }

  "reverseMap" should "return a Set containing a UserNamePrincipal for an existing Uid" in {
    ldapPlugin.reverseMap(new UidPrincipal("50999")) should contain (new UserNamePrincipal("testuser"))
  }

  it should "return a serializable Set" in {
    val set = ldapPlugin.reverseMap(new UidPrincipal("50999"))
    set.isInstanceOf[java.io.Serializable] should be (true)
  }

  it should "throw an NoSuchPrincipalException for a non existent Uid" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.reverseMap(new UidPrincipal("666"))
    }
  }

  it should "return a Set containing a GroupNamePrincipal for an existing Gid" in {
    ldapPlugin.reverseMap(new GidPrincipal("3752", true)) should contain (new GroupNamePrincipal("htw-berlin"))
  }

  it should "throw a NoSuchPrincipalException for a non existent Gid" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.reverseMap(new GidPrincipal("51000", true))
    }
  }

  "session" should "return the user's home and root directory, and the access rights" in {
    val attr = new java.util.HashSet[AnyRef]()
    ldapPlugin.session(setAsJavaSet(Set[Principal](new UserNamePrincipal("testuser"))), attr)

    attr should have size 3
    attr should contain (new HomeDirectory("/root"))
    attr should contain (new RootDirectory("/root/dcache-cloud/testuser/home"))
  }

}

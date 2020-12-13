package Slave

object FileManager {
  def readAllInput(): AddressBook =
    Using(new FileInputStream("addressbook.pb")) { fileInputStream =>
      AddressBook.parseFrom(fileInputStream)
    }.recover {
      case _: FileNotFoundException =>
        println("No address book found. Will create a new file.")
        AddressBook()
    }.get
}
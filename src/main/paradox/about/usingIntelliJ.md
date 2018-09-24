#A note on IntelliJ
Using a cross-compiled project in IntelliJ has some issues (citation needed).

For now, I've just been manually fixing it every time I change the build.sbt, but at
some point I'll have to script it properly.

The issue is with shared source roots, and so for the core and JS cross-projects I:

- Import the project into IntelliJ
- Open the Project Structure
- **Remove** the "riff-core-sources" and "riff-json-sources" modules
- Choose to "Add Content Root: to the riff-core-jvm module, and add the riff-core/shared/src folder
- Change the 'test' folder to a test source (not main source)

It's a rare enough occurrence to not be a problem, but worth noting here if anyone should want to open this
project in IJ.


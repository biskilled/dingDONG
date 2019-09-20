.. _tag_vc:

SOURCE CONTROL
==============

dingDONG provides full metadata version control mechanism.
On identified object changes such as new, update or delete - meta-data is stored at dingDONG DB. Version numbers update automatically by execution sequential number.

For example, if execution level is set to ``1`` and new objects found, dingDONG will mark new release <main_version>.<new_release> and store all created object scripts related to current release number. A new release will be added when a change is detected. The main version is managed at the configuration file.

To enable dingDONG versioning you have to initialize version number and set dingDONG MongoDB repository.

More details can be found under install page

MACHINE LEARNING OR EXTERANL CODE
#################################

dingDONG using GIT embedded functionality for managing folder and files content versions.
It has full integration with ``GITHUB`` repository which stores all revisions

The release is defined by local folder structure.
Release usage sample -
    - ``C:\ML`` defined as the main root for local code.
    - The release will increase on any new update at the folder or file directly under ``C:\ML`` path.
        - Changes under certain folder will be considered as a new release
        - Changes under file directly under ``C:\ML`` will be considered as a new release

MACHINE LEARNING RESULTS
########################

dingDONG store experience results related to each release at dingDONG DB.
Results data-set can be defined by developers and divided into two main properties:

- Shared measures - calculated measures for any experience
   - Sample: ``count total inserted rows``, ``count total output rows``, ``total execution time`` ...
- Private measures - shares measures for all experiments
    - Sample: ``meusre1``
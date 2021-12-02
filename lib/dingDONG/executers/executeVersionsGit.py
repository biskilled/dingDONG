# Copyright (c) 2017-2021, BPMK LTD (BiSkilled) Tal Shany <tal.shany@biSkilled.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#
# This file is part of dingDong
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.

### USING gitpython and pygithub

import os
import sys
import traceback
import base64
import shutil
import time
import stat
import io
import getpass
import git  # GitPython
from github import Github  # PyGithub

from dingDONG.misc.logger   import p
from dingDONG.misc.enums import eConn, eGit
from dingDONG.conn.connDB import connDb
from dingDONG.config import config

class gitMng():
    def __init__ (self, remoteType, repoName, repoLocalFolder, remoteAuth,
                  create=True, defaultBranch="master"):

        # GIT, GITHUB packages
        import git  # GitPython
        from github import Github  # PyGithub

        self.remoteType = remoteType
        if self.remoteType.lower() not in (eGit.ALL_TYPES):
            p ("REPO TYPE %s is not defined " %self.remoteType)

        self.repoName   = repoName

        self.remoteObj      = None
        self.remoteRepo     = None
        self.remoteUrl      = None
        self.remoteUrlFull  = None
        self.remoteAuth     = remoteAuth
        self.remoteConnected= False
        self.remoteUser     = None
        self.localObj       = None
        self.localRepo      = None
        self.repoLocalFolder = repoLocalFolder
        self.localPath      = os.path.join (self.repoLocalFolder, repoName)
        self.localPathTmp   = os.path.join (self.repoLocalFolder, "temp")
        self.localPathHistory = os.path.join (self.repoLocalFolder, "History_"+repoName)
        self.localConnected = False
        self.create         = create
        self.branch         = defaultBranch

        self.versionFile    = 'version.txt'
        self.startTime      = time.strftime("%Y-%m-%d %H:%M:%S")
        self.versionId      = None
        self.getSetVersion (versionCommit=None, defualtStart='1')

    def initRepos (self, commDesc="Init Repo .... "):
        totalNewFiles = 0
        if not self.localConnected:     self.connectLocal ()
        if not self.remoteConnected:    self.connectRemote()

        if self.remoteConnected and not self.remoteRepo:
            if self.create:
                self.remoteRepo = self.remoteUser.create_repo(self.repoName, description=commDesc,
                                                              has_wiki=False, has_issues=True, auto_init=False)
                self.repoName = self.remoteRepo.name
                self.remoteUrl = self.remoteRepo.git_url
                startFrom = self.remoteUrl.find("://")
                self.remoteUrlFull = 'https://%s' % (self.remoteUrl[startFrom + 3:])
                self.remoteRepo.create_file(".gitignore", "Add gitIgnore file", self.__addGitIgnore(), branch="master")
                p ("REMOTE REPO %s CREATED !!!!" %self.repoName)
            else:
                p("REMOTE REPO %s NOT CREATED" %self.repoName, "e")

        if self.localConnected and not self.localRepo:
            if self.create:
                _ = git.Repo.init(self.localPath)
                self.localRepo = git.Repo(self.localPath)
                origin = self.localRepo.create_remote('origin', url=self.remoteUrlFull)
                origin.fetch()
                self.localRepo.create_head("master", origin.refs.master)
                self.localRepo.heads.master.set_tracking_branch(origin.refs.master)
                self.localRepo.heads.master.checkout(True)
                newFiles = self.localRepo.untracked_files
                totalNewFiles = len(newFiles)
                self.localRepo.index.add(newFiles)
                commitId = self.localRepo.index.commit(commDesc)
                self.getSetVersion(versionCommit=commitId)
                self.localRepo.git.push(force=True)
        return totalNewFiles

    def connectLocal (self):
        if self.localPath and os.path.isdir(self.localPath):
            try:
                _ = git.Repo(self.localPath).git_dir
                self.localRepo = git.Repo(self.localPath)
                p("SET: USING LOCAL REPO %s, URL: %s" % (self.repoName, self.localPath))
            except git.exc.InvalidGitRepositoryError as e:
                self.localRepo = None
        if self.localPath:
            if not os.path.isdir(self.localPath):
                os.mkdir(self.localPath)
            self.localConnected = True
        else:
            p ("PATH %s IS NOT EXISTS " %self.localPath , "e")

    def connectRemote (self, commDesc="Init repo"):
        if eGit.GITHUB == self.remoteType:
            try:
                remoteGit = Github(self.remoteAuth)
                p("INIT: CONNECTED TO GITHUB USING TOKEN: %s  " % (self.remoteAuth))
                self.remoteUser = remoteGit.get_user()
                self.remoteConnected = True
                repo = self.__getRemoteRepo(repos=self.remoteUser.get_repos())
                if repo:
                    self.repoName = repo.name
                    self.remoteUrl = repo.git_url
                    # self.remoteUrlFull
                    startFrom = self.remoteUrl.find("://")
                    self.remoteUrlFull = 'https://%s@%s'  %(self.remoteAuth, self.remoteUrl[startFrom+3:])
                    self.remoteRepo = repo
                    p("SET: USING REMOTE GITHUB REPO %s, URL: %s" % (self.repoName, self.remoteUrl))

            except Exception as e:
                p("CREATE REPO: REMOTE GIT IS NOT CONNECTED !")
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=4, file=sys.stdout)
                p("Error: %s" % (str(e)))

    def __getRemoteRepo (self,repos):
        for repInRemote in repos:
            if repInRemote.name.lower() == self.repoName.lower():
                return repInRemote

    def __addGitIgnore (self):
        str = """__pycache__/\n*.py[cod]\n*$py.class\n\n.Python\nenv/\nbuild/\ndevelop-eggs/\n
                dist/\ndownloads/\neggs/\n.eggs/\nlib64/\nparts/\nsdist/\nvar/\nwheels/\n*.egg-info/\n
                .installed.cfg\n*.egg\n*.manifest\n*.spec"""
        return str

    def getRemoteRepo (self):
        createDesc = 'Local testi ng repo '

        if not self.remoteConnected:self.connectRemote()
        if not self.localConnected: self.connectLocal()

        if not self.remoteObj:
            p ("GET REMOTE: REMOTE GIT IS NOT CONNECTED !")
            return
        try:
            if self.remoteRepo:
                p ("GET REMOTE: REMOTE REPO %s EXISTS, URL: %s" %(self.repoName, self.remoteRepo.git_url ))

            elif not self.remoteRepo and create:
                self.remoteRepo = self.remoteUser.create_repo(self.repoName, description=createDesc,has_wiki=False,has_issues=True,auto_init=False)
                self.remoteUrl  = self.remoteRepo.git_url
                startFrom = self.remoteUrl.find("://")
                self.remoteUrlFull = 'https://%s' % (self.remoteUrl[startFrom + 3:])


                ### Adding sample file to check
                self.remoteRepo.create_file("src/test.txt", "test", "test", branch="master")
                p ("GET REMOTE: REPO %s CREATED, URL: %s" %(self.remoteRepo.name, self.remoteUrl))

                self.__deleteFolder(fPath=self.localPath, totalRetry=4)

            if not os.path.isdir (self.localPath):
                ## Clone Repo
                localRepo = git.Repo.clone_from(self.remoteUrlFull, self.localPath)
                localRepo.close()

                p ("GET LOCAL: CLONED FROM REMOTE TO FOLER %s " %self.localPath)
                #git.Git(self.repoFolder).clone(self.remoteUrl )

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,limit=4, file=sys.stdout)
            p ("Error: %s" %(str(e)))

    def deleteRemoteRepo(self, repoName=None, totalRetry=4):
        if not self.remoteConnected:
            self.connectRemote()

        if self.remoteRepo:
            self.remoteRepo.delete()
            p ("DELETED GITHUB REPO %s " %(self.repoName))
        else:
            p("DELETED: NOT FOUND GITHUB REPO %s " %(self.repoName))
        self.remoteConnected = False

        if not self.localConnected:
            self.connectLocal()

        if self.localRepo:
            self.__deleteFolder(fPath=self.localPath, totalRetry=totalRetry)
        else:
            p("DELETED: NOT FOUND LOCAL REPO %s " % (self.localPath))

        self.localConnected = False

    def __extractFolderHeader(self, fileList, existsDict=None, folderLevel=1):
        if not fileList or len (fileList)<1:
            return existsDict

        fileList = fileList if isinstance(fileList, (list,tuple)) else [fileList]
        existsDict = {} if existsDict is None else existsDict

        # minimum folders to add commit
        for file in fileList:
            head, tail = os.path.split(file)
            if head not in existsDict:
                existsDict[head] = []
            existsDict[head].append( tail)
        return existsDict

    def getCommitId (self, vId):
        if not os.path.isfile(self.versionFile):
            p("FILE NOT EXISTS ")
            return
        with io.open(self.versionFile, 'r') as f:
            for i, line in enumerate(f):
                row = line.split(",")
                if str(row[0]).lower() == str(vId).lower():
                    return row[1]
        p("CANNOT FIND VERSION %s" %(vId) )
        return

    def getSetVersion (self, versionCommit=None, defualtStart='1'):
        fileTmp         = self.versionFile
        startVersion    = defualtStart
        ##commitRandom    = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        existsVersion   = {}
        lastOne         = startVersion

        if not os.path.isfile(fileTmp):
            self.versionId = startVersion
            if versionCommit:
                with io.open(fileTmp, 'w') as f:
                    firstLine = '%s, %s \n' % (startVersion, versionCommit)
                    f.write(firstLine)
        else:
            # get all existing versions
            if not self.versionId:
                with io.open(fileTmp, 'r') as f:
                    for i, line in enumerate(f):
                        r = line.split(",")
                        if r[0] not in existsVersion:
                            existsVersion[r[0]] = None
                        existsVersion[r[0]] = r[1]
                        lastOne = r[0]
                self.versionId = str(int(lastOne) + 1)

            # add new version
            if versionCommit:
                with io.open(fileTmp, 'a', encoding='utf-8') as f:
                    newCommit = '%s, %s \n' % (self.versionId, versionCommit)
                    f.write(newCommit)
                    self.versionId = str(int(self.versionId) + 1)

    def gitUpdate (self, commMsg=None):
        commDesc = commMsg if commMsg else "INIT REPOSITORY ..."
        newFiles = self.initRepos(commDesc=commDesc)

        # Update project
        self.localRepo.git.pull()

        # Commit all prev changes
        self.__pushCommits()

        # Check for changes
        self.__checkLocalUpdates (commMsg=commMsg)


    def __pushCommits (self):
        # Commits that not pushed
        commitsNotPush = self.localRepo.iter_commits('origin/%s..%s' %(self.branch, self.branch))
        countCommits = sum(1 for c in commitsNotPush)

        if countCommits is not None and countCommits>0:
            self.localRepo.git.push(force=True)  # self.remoteUrlFull,
            p(">>>> %s COMMITS PUSHED TO REMOTE " %(str(countCommits)))

    def __isFolder (self, file):
        head, tail = os.path.split(file)
        if not head or head=='':
            return True
        return False

    def __checkLocalUpdates (self, commMsg=None):

        """" GIT modifying files mode
                A: adding path
                D: deleted paths
                R: renamed paths
                M: paths with modified data
                T: changed in the type paths
        """

        addFiles        = []
        addFolder       = []
        deletedFiles    = []
        deletedFolders  = []
        newFilesAndFolders= self.localRepo.untracked_files
        for f in newFilesAndFolders:
            addFolder.append(f) if self.__isFolder(file=f) else addFiles.append(f)

        for item in self.localRepo.index.diff(None):
            if item.change_type in ['A','R','M','T']:
                addFolder.append (item.a_path) if self.__isFolder(file=item.a_path) else addFiles.append(item.a_path)
            else:
                deletedFolders.append (item.a_path) if self.__isFolder(file=item.a_path) else deletedFiles.append(item.a_path)

        commDescF = ""
        if len (addFolder)>0:
            self.localRepo.index.add(addFolder)
            commDescF+="TOTAL MODIFIED: %s " %(str(len(addFolder )))

        if len (deletedFolders)>0:
            self.localRepo.index.remove(items=deletedFolders, cached=True)
            commDescF += "    ;   TOTAL DELETED: %s ;" % (str(len(deletedFolders)))

        if len(commDescF)>0:
            p(">>>>>    COMMIT::: %s, MSG: %s" % (commDescF, commMsg))
            comm = commMsg if commMsg else commDescF
            commitId = self.localRepo.index.commit(comm)
        else:
            p(">>>>>    NO FOLDERS TO COMMIT")

        commDesc = ""
        if len(addFiles) > 0:
            self.localRepo.index.add(addFiles)
            commDesc += "TOTAL MODIFIED: %s " % (str(len(addFolder)))

        if len(deletedFiles) > 0:
            self.localRepo.index.remove(items=deletedFiles, cached=True)
            commDesc += "    ;   TOTAL DELETED: %s ;" % (str(len(deletedFiles)))

        if len(commDesc) > 0:
            p(">>>>>    COMMIT::: %s, MSG: %s" % (commMsg, commDesc))
            comm = commMsg if commMsg else commDesc

        if len(commDesc) > 0 or len(commDescF) > 0:
            commitId = self.localRepo.index.commit(comm)
            self.getSetVersion(versionCommit=commitId)
            self.localRepo.git.push(force=True)

    def checkChanges (self):
        if not self.localConnected: self.connectLocal()
        if not self.remoteConnected: self.connectRemote()

        # Update project
        self.localRepo.git.pull()

        # Commits that not pushed
        commitsNotPush = self.localRepo.iter_commits('origin/master..master')
        countCommits = sum(1 for c in commitsNotPush)

        if countCommits is not None and countCommits>0:
            p("THER ARE %s COMMITS THAT ARE NOT PUSHED" %(str(countCommits)))
            self.localRepo.git.push(force=True) # self.remoteUrlFull,

        """" GIT modifying files mode
                A: adding path
                D: deleted paths
                R: renamed paths
                M: paths with modified data
                T: changed in the type paths
        """

        changedFiles = []
        deletedFiles = []
        newFiles     = self.localRepo.untracked_files

        for item in self.localRepo.index.diff(None):
            if item.change_type in ['A','R','M','T']:
                changedFiles.append(item.a_path)
            else:
                p("FILE %s IS DELETED" %(item.a_path))
                deletedFiles.append (item.a_path)

        filesToCommitDict = self.__extractFolderHeader(fileList=changedFiles,   existsDict=None, folderLevel=1)
        filesToCommitDict = self.__extractFolderHeader(fileList=newFiles,       existsDict=filesToCommitDict, folderLevel=1)
        filesToCommitDict = self.__extractFolderHeader(fileList=deletedFiles,   existsDict=filesToCommitDict, folderLevel=1)

        if filesToCommitDict and len(filesToCommitDict)>0:
            for fileLocation in filesToCommitDict:
                if fileLocation == '':
                    for filePostFix in filesToCommitDict[fileLocation]:
                        if filePostFix in changedFiles:
                            fileStatus = gitTypes.MODIFIED
                            self.localRepo.index.add([filePostFix])
                        elif filePostFix in deletedFiles:
                            fileStatus = gitTypes.DELETED
                            self.localRepo.index.remove(items=[filePostFix], cached=True)
                        elif filePostFix in newFiles:
                            fileStatus = gitTypes.NEW
                            self.localRepo.index.add([filePostFix])
                        else:
                            fileStatus = gitTypes.UNKNOWN

                        commitMsg = gitTypes.COMMIT_MSG %(self.versionId, self.startTime, fileStatus, filePostFix)
                        p("COMMIT::: %s" %(commitMsg ))

                        commitId = self.localRepo.index.commit(commitMsg)
                        self.getSetVersion( versionCommit=commitId )


                        self.localRepo.git.push(force=True)
                ## Commiting project folders
                else:
                    tChangedFiles = []
                    tdeletedFiles = []
                    fileStatus = gitTypes.UNKNOWN

                    for filePostFix in filesToCommitDict[fileLocation]:
                        gitFileLocation = os.path.join (fileLocation, filePostFix)
                        gitFileLocation = gitFileLocation.replace('\\','/')
                        if gitFileLocation in changedFiles:
                            fileStatus = gitTypes.MODIFIED
                            tChangedFiles.append ( gitFileLocation )
                        elif gitFileLocation in deletedFiles:
                            fileStatus = gitTypes.DELETED
                            tdeletedFiles.append( gitFileLocation )
                        elif gitFileLocation in newFiles:
                            fileStatus = gitTypes.NEW
                            tChangedFiles.append( gitFileLocation )
                        else:
                            p("FILE TYPE IS NOT DEFINED: %s" %(gitFileLocation))
                            tChangedFiles.append( gitFileLocation )

                    filesMsg = 'MODIFIED: %s ' %(str(",".join(tChangedFiles))) if len(tChangedFiles)>0 else ''
                    filesMsg += ';DELETED: %s' % (str(",".join(tdeletedFiles))) if len(tdeletedFiles) > 0 else ''

                    if len(tdeletedFiles)>0:
                        self.localRepo.index.remove(items=tdeletedFiles, cached=True)

                    if len(tChangedFiles)>0:
                        self.localRepo.index.add(items=tChangedFiles)

                    commitMsg = gitTypes.COMMIT_MSG % (self.versionId, self.startTime, fileStatus, filesMsg)
                    p("COMMIT::: %s" % (commitMsg))

                    commitId = self.localRepo.index.commit(commitMsg)
                    self.getSetVersion(versionCommit=commitId)
                    self.localRepo.git.push(force=True)
        else:
            p("THERE ARE NO FILES TO COMMITS")

    def getVesrion (self, vId):
        if not self.localConnected:  self.connectLocal()
        if not self.remoteConnected: self.connectRemote()

        if not os.path.isdir(self.localPathHistory):
            os.mkdir(self.localPathHistory)

        commitId = self.getCommitId(vId=vId)

        if commitId:
            p("CHECKOUT TO VERSION %s, COMMIT %s" %(vId, commitId))
            #self.localRepo.git.checkout ( commitId.strip() )

            ### local git
            #changedFIles = self.localRepo.git.diff_tree (commitId.strip(), no_commit_id=True, name_only=True, r=True).split("\n")

            ### remote Git -gitHub
            changedFIles = self.remoteRepo.get_commit(sha=commitId.strip()).files

            dirToLoad = []

            for cF in changedFIles:
                head, tail = os.path.split(cF.filename)
                if head and len(head)>0:
                    print (head)
                    self.download_directory(repository=self.remoteRepo, sha=commitId.strip(), server_path=head)


            currentCommit = self.localRepo.iter_commits (rev=commitId.strip())

            #print("Tal 2")
            #for comm in currentCommit:
            #    print ("TTT", comm)
            #    print ("YYYY",comm.tree)
            #    print ("vvvvv", comm.parents)

    def __deleteFolder (self, fPath, totalRetry=4):
        retry = 0
        if fPath:
            if os.path.isdir(fPath):
                #if not os.access(fPath, os.W_OK):
                for root, dirs, files in os.walk(fPath):
                    for momo in dirs:
                        os.chmod(os.path.join(root, momo), stat.S_IWUSR)
                    for momo in files:
                        os.chmod(os.path.join(root, momo), stat.S_IWUSR)


                    # Is the error an access error ?


                while retry < totalRetry:
                    retry += 1
                    try:
                        shutil.rmtree(fPath)
                        p("DELETED LOCAL FOLDER %s" % (fPath))
                        retry = totalRetry + 1
                    except Exception as e:
                        p("TRY %s OUT OF %s, ERROR DELETE %s " % (str(retry), str(totalRetry), str(fPath)))
                        p(e)
                        time.sleep(1)
            else:
                p("%s IS NOT EXISTS OR NOT FOLDER  " %(fPath))

    def get_sha_for_tag(self, repository, tag):
        """
        Returns a commit PyGithub object for the specified repository and tag.
        """
        branches = repository.get_branches()
        matched_branches = [match for match in branches if match.name == tag]
        if matched_branches:
            return matched_branches[0].commit.sha

        tags = repository.get_tags()
        matched_tags = [match for match in tags if match.name == tag]
        if not matched_tags:
            raise ValueError('No Tag or Branch exists with that name')
        return matched_tags[0].commit.sha

    def download_directory(self, repository, sha, server_path):
        """
        Download all contents at server_path with commit tag sha in
        the repository.
        """
        contents = repository.get_contents(server_path, ref=sha)

        for content in contents:
            print ("Processing %s" % content.path)
            if content.type == 'dir':
                self.download_directory(repository, sha, content.path)
            else:
                try:
                    path = content.path
                    file_content = repository.get_contents(path, ref=sha)
                    file_data = base64.b64decode(file_content.content)
                    file_out = open(os.path.join (self.localPathHistory, content.name), "wb")
                    file_out.write(file_data)
                    file_out.close()
                    print (os.path.join (self.localPath, content.name))
                except Exception as exc: # (GithubException, IOError)
                    p('Error processing %s: %s' %(content.path, exc))

class dbVersions ():
    def __init__ (self, folder=None, vFileName=None, vFileData=None, url=None, conn=eConn.types.SQLSERVER, tbl=None):
        self.isValidFile = False
        self.isValidDb   = False
        self.folder     = None
        self.vFile      = None
        self.vFileData  = None

        self.db         = None
        self.tbl        = None

        self.version    = config.VERSION
        self.currentUser= getpass.getuser()
        self.currentTime =time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        if self.version:
            if folder and os.path.isdir(folder) and vFileData and vFileName:
                self.folder     = folder
                self.vFileName  = os.path.join(folder, vFileName)
                self.vFileData  = os.path.join(folder, vFileData)
                self.isValidFile = self.__setVersionFromFile()

                p("FILE VERSION IS ACTIVATED, VERSION:%s LOGGD INTO: %s" %(self.version, str(vFileData)),"i")

            if conn and url and tbl:
                self.db     = connDb(connType=conn, connUrl=url)
                self.tbl    = tbl
                self.isValidD= self.__setVersionFromDb()

                p("DB VERSION IS ACTIVATED, VERSION:%s LOGGD INTO TABLE: %s" % (self.version, str(tbl)),"i")

    def __setVersionFromFile (self):
        try:
            if not os.path.isfile(self.vFileName):
                self.version = config.VERSION
                with open(self.vFileName, 'w') as f:
                    f.write('%s\n' % str(self.version))
            else:
                with open(self.vFileName, 'r+') as f:
                    lines = f.read().splitlines()
                    curr_version = lines[-1]
                    self.version = str(int(curr_version) + 1)
                    f.write('%s\n' % (self.version))
            return True

        except Exception as e:
            p("ERROR: %s"  %(e))
            return False

    def __setVersionFromDb(self):
        try:
            self.version = config.VERSION
            return True
        except Exception as e:
            p("ERROR: %s"  %(e))
            return False

    def addCS (self, changeSet):
        if self.isValidFile:
            self.__addChangeSetToFile(changeSet=changeSet)

        if self.isValidDb:
            self.__addChangeSetToDb (changeSet=changeSet)

    def __addChangeSetToFile (self, changeSet):
        with open (self.vFileData, "a") as f:
            for cs in changeSet:
                f.write('%s,%s,%s' %(self.currentTime, self.currentUser, cs))

    def __addChangeSetToDb (self, changeSet):
        p("NOT IMPLEMENTED YET .... %s" %(str(changeSet)) )


## TEST #######
# Create token by creting new token at setting/Developer setting/Personal access token
# Cretae new project --> Add file
# Connect / load first project !!!
# cd ...
# git clone ...
# add file, git add .  ; git commit -m "...." ; git push

repoName    = "exp1"
repoToken   = ""
localPath   = ""

mm = gitMng(remoteType=eGit.GITHUB, repoName=repoName, repoLocalFolder=localPath,
            remoteAuth=repoToken, create=True, defaultBranch="master")
mm.gitUpdate()
#mm.getVesrion (vId=2)


#mm.deleteRemoteRepo()

#mm.initRepos (commDesc="Init Repo .... ")

#myDevOps = ops(repoName=repoName, repoFolder=localPath, remoteToken=repoToken)
#myDevOps.getRemoteRepo (create=True)
#myDevOps.checkChanges ()



#myDevOps.checkChanges ()

#myDevOps.getVesrion (vId=1)
#myDevOps.getVesrion (vId=2)
#myDevOps.getVesrion (vId=3)
#myDevOps.getVesrion (vId=4)


############################################################################################################

#updateVersion (versionCommit=True)
#newRepo = 'my-new-repo'
#repo_dir = os.path.join('C:\\gitHub\\', newRepo)
#file_name = os.path.join(repo_dir, 'new-file')

#r = git.Repo.init(repo_dir)
# This function just creates an empty file ...
#open(file_name, 'wb').close()
#r.index.add([file_name])
#r.index.commit("initial commit")
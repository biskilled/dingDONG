# Copyright (c) 2017-2019, BPMK LTD (BiSkilled) Tal Shany <tal.shany@biSkilled.com>
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


from dingDONG.misc.logger   import p
from dingDONG.misc.enums import eConn
from dingDONG.conn.connDB import connDb
from dingDONG.config import config

class gitTypes (object):
    MODIFIED    = 'MODIFIED'
    NEW         = 'NEW'
    DELETED     = 'DELETED'
    UNKNOWN     = 'WHAT IS IT ....'

    COMMIT_MSG = '%s: TIME:%s, STATUS:%s, NAME: %s'

class ops():
    def __init__ (self, repoName, repoFolder, remoteUser, remotePass):
        # GIT, GITHUB packages
        import git
        from github import Github

        self.repoName   = repoName
        self.repoFolder = repoFolder

        self.remoteObj      = None
        self.remoteRepo     = None
        self.remoteUrl      = None
        self.remoteUrlFull  = None
        self.remoteUser     = None
        self.remoteLoginUser= remoteUser
        self.remoteLoginPass= remotePass
        self.remoteConnected= False

        self.localObj       = None
        self.localRepo      = None
        self.localPath      = os.path.join(self.repoFolder, self.repoName)
        self.localConnected = False

        self.versionFile = 'version.txt'
        self.startTime = time.strftime("%Y-%m-%d %H:%M:%S")
        self.versionId = None
        self.getSetVersion (versionCommit=None, defualtStart='1')

    def connectLocal (self):
        if self.localPath and os.path.isdir(self.localPath):
            try:
                _ = git.Repo(self.localPath).git_dir
                self.localRepo = git.Repo(self.localPath)
                p("SET: USING LOCAL REPO %s, URL: %s" % (self.repoName, self.localPath))
                self.localConnected = True
            except git.exc.InvalidGitRepositoryError as e:
                p("SET: ERROR USING LOCAL FOLDER ")
                p("SET: ERROR %s " %(str(e)) )
                self.localRepo = None
                self.localConnected = False

    def connectRemote (self):
        try:
            self.remoteObj = Github(self.remoteLoginUser, self.remoteLoginPass)
            p("INIT: CONNECTED TO GITHUB USING USER: %s, PASS: %s " % (self.remoteLoginUser, self.remoteLoginPass))
            self.remoteUser = self.remoteObj.get_user()

            for repo in self.remoteUser.get_repos():
                if repo.name.lower() == self.repoName.lower():
                    self.repoName = repo.name
                    self.remoteUrl = repo.git_url

                    # self.remoteUrlFull
                    startFrom = self.remoteUrl.find("://")
                    self.remoteUrlFull = 'https://%s:%s@%s'  %(self.remoteLoginUser, self.remoteLoginPass, self.remoteUrl[startFrom+3:])
                    self.remoteRepo = repo
                    p("SET: USING REMOTE GITHUB REPO %s, URL: %s" % (self.repoName, self.remoteUrl))
                    self.remoteConnected = True
                    break

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=4, file=sys.stdout)
            p("Error: %s" % (str(e)))
            self.remoteConnected = True

    def getRemoteRepo (self, create=True):
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

    def deleteRemoteRepo(self, totalRetry=4):
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

    def checkChanges (self):
        if not self.localConnected: self.connectLocal()
        if not self.remoteConnected: self.connectRemote()

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
        if not self.localConnected: self.connectLocal()
        if not self.remoteConnected: self.connectRemote()
        commitId = self.getCommitId (vId=vId)

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
        contents = repository.get_dir_contents(server_path, ref=sha)

        for content in contents:
            print ("Processing %s" % content.path)
            if content.type == 'dir':
                self.download_directory(repository, sha, content.path)
            else:
                try:
                    path = content.path
                    file_content = repository.get_contents(path, ref=sha)
                    file_data = base64.b64decode(file_content.content)
                    file_out = open(content.name, "w")
                    print (path)
                    #file_out.write(file_data)
                    file_out.close()
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
#repoName="talTest2"
#localPath = "C:\\gitHub"

#myDevOps = ops(repoName=repoName, repoFolder=localPath, remoteUser='biskilled', remotePass='Emili1217')
#myDevOps.deleteRemoteRepo()
#myDevOps.getRemoteRepo (create=True)

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
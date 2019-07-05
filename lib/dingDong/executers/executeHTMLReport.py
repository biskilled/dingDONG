# create HTML

class eHtml (object):
    HEADER  = "header"
    ROWS    = "rows"

def createHtmlFromList(htmlList, htmlHeader=None):
    htmlRow = "\t\t<h1>%s</h1>\n" %htmlHeader if htmlHeader else ""
    htmlHeader = """
            table {
                border-collapse: collapse; 
                border:1px solid #69899F;} 
            table td{
                border:1px dotted #000000;
                padding:5px;}
            table td:first-child{
                border-left:0px solid #000000;}
            table th{
               border:2px solid #69899F;
               padding:5px;}"""

    htmlHeader = "\t<style>%s\n\t\t</style>" %htmlHeader

    htmlHeader = """
    <header>
    %s
    </header>""" %(htmlHeader)

    htmlBody = ""
    for tblDic in htmlList:
        lHeader     = None
        lRows       = None
        tblHeader   = ""
        tblLines    = ""

        if eHtml.HEADER in tblDic:
            lHeader = tblDic[eHtml.HEADER]
            for h in lHeader:
                tblHeader += '<th>%s</th>' % str(h)

            tblHeader = """
            <thead>
                <tr>
                %s
                </tr>
            </thead>""" %tblHeader

        if eHtml.ROWS in tblDic:
            lRows   = tblDic[eHtml.ROWS]
            for row in lRows:
                tblLinesTD = ""
                for col in row:
                    strRow = ""
                    if isinstance(col, (list,tuple)):
                        for c in col:
                            strRow+= "%s <br>" %str(c)
                    else:
                        strRow = str(col)
                    tblLinesTD += '<td>%s</td>' %strRow
                tblLines+=  "<tr>\n\t\t\t\t\t%s\n\t\t\t\t</tr>\n\t\t\t\t" %tblLinesTD
            tblLines = """
            <tbody class="allign-center">
                %s
            </tbody>    
            """ %tblLines

        tblHtml = '\t\t<table class="table table-bordered">%s%s\n\t\t</table>' %(tblHeader, tblLines)

        htmlBody+='%s\n\t<br>\n' %tblHtml

    htmlBody = "\t<body>\n%s%s</body>" %(htmlRow,htmlBody)

    htmlStr = "<html>%s\n%s\n</html>" %(htmlHeader, htmlBody)

    return htmlStr

#htmlList = [{'header':['col1','col2','col3','col4'],'rows':[['a1','a2','a3','a4'],['b1','b2','b3','b4'],['c1','c2','c3','c4']]},
#            {'header':['col1','col2','col3','col4'],'rows':[['a1','a2','a3','a4'],['b1','b2','b3','b4'],['c1','c2','c3','c4']]}]
#xx = createHtmlFromList(htmlList, "hallo world")
#print (xx)





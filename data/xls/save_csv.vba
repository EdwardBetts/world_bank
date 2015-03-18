Public Sub SaveWorksheetsAsCsv()
Dim WS As Excel.Worksheet
Dim SaveToDirectory As String
    SaveToDirectory = "C:\temp\"
    For Each WS In ThisWorkbook.Worksheets
        WS.SaveAs SaveToDirectory & WS.Name, xlCSV
    Next
End Sub

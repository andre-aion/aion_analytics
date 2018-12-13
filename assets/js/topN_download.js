var data = source.data;
filetext = 'miner_addr,percentage'+String.fromCharCode(10);
for (i=0; i < data['miner_addr'].length; i++) {
    var currRow = [data['miner_addr'][i].toString(),
                   data['percentage'][i].toString().concat('\\n')];

    var joined = currRow.join();
    filetext = filetext.concat(joined);
}

var filename = 'Top_N_miners.csv';
var blob = new Blob([filetext], { type: 'text/csv;charset=utf-8;' });

//addresses IE
if (navigator.msSaveBlob) {
    navigator.msSaveBlob(blob, filename);
}

else {
    var link = document.createElement("a");
    link = document.createElement('a')
    link.href = URL.createObjectURL(blob);
    link.download = filename
    link.target = "_blank";
    link.style.visibility = 'hidden';
    link.dispatchEvent(new MouseEvent('click'))
}


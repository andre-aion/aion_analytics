var data = source.data;
filetext = 'miner_address,block_date,block_number,approx_value'+String.fromCharCode(10);
for (i=0; i < data['miner_address'].length; i++) {
    var currRow = [data['miner_address'][i].toString(),
                   data['block_date'][i].toString(),
                   data['block_number'][i].toString(),
                   data['approx_value'][i].toString().concat('\\n')];

    var joined = currRow.join();
    filetext = filetext.concat(joined);
}

var filename = 'Tier1_miners.csv';
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


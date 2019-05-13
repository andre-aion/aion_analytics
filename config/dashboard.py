from datetime import datetime, timedelta

from bokeh.models import HTMLTemplateFormatter

DATEFORMAT = '%Y-%m-%d %H:%M:%S'
config = {

    'dates': {
        'DATEFORMAT': '%Y-%m-%d %H:%M:%S',
        'last_date': datetime.today() - timedelta(days=1),
        'current_year_start':datetime(datetime.today().year,1,1,0,0,0),
        'DAYS_TO_LOAD':30
    },
    'colors': ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231',
               '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe',
               '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000',
               '#aaffc3', '#808000', '#ffd8b1', '#000075', '#808080',
               '#ffffff', '#000000'],
    'formatters':
        {
        'Unlikely': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Unlikely == 'string'){
                            var val = Unlikely.split(' ');
                            val = val.slice(-1)[0] 
                            console.log(val);
                            if(val < thresh_1 )
                                {return("#1aff1a")}
                            else if (val < thresh_2)
                                {return("#CCCC00")}
                            else
                                {return("#ff6633")}
                        }
                        else{
                            if(Unlikely < thresh_1 )
                                {return("#1aff1a")}
                            else if (Unlikely < thresh_2)
                                {return("#CCCC00")}
                            else if (Unlikely >= thresh_2)
                                {return("#ff6633")}
                        }

                    }()) %>;
                    color: #111111;font-size:2em;text-align:center">
                <%= value %>
                </div>
            """),
            'Seldom': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Seldom == 'string'){
                           var val = Seldom.split(' ');
                            val = val.slice(-1)[0] 
                            console.log(val);
                            if(val < thresh_1 )
                                {return("#1aff1a")}
                            else if (val < thresh_2)
                                {return("#CCCC00")}
                            else
                                {return("#ff6633")}
                        }
                        else{
                            if(Seldom < thresh_1 )
                                {return("#1aff1a")}
                            else if (Seldom < thresh_2)
                                {return("#CCCC00")}
                            else if (Seldom >= thresh_2)
                                {return("#ff6633")}
                        }

                    }()) %>;
                    color: #111111;font-size:2em;text-align:center">
                <%= value %>
                </div>
            """),

            'Occaisional': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Occaisional == 'string'){
                            var val = Occaisional.split(' ');
                            val = val.slice(-1)[0] 
                            console.log(val);
                            if(val < thresh_1 )
                                {return("#1aff1a")}
                            else if (val < thresh_2)
                                {return("#CCCC00")}
                            else
                                {return("#ff6633")}
                        }
                        else{
                            if(Occaisional < thresh_1 )
                                {return("#1aff1a")}
                            else if (Occaisional < thresh_2)
                                {return("#CCCC00")}
                            else 
                                {return("#ff6633")}
                        }

                    }()) %>;
                    color:#111111;font-size:2em;text-align:center">
                <%= value %>
                </div>
            """),
            'Likely': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Likely == 'string'){
                            var val = Likely.split(' ');
                            val = val.slice(-1)[0] 
                            if(val < thresh_1 )
                                {return("#1aff1a")}
                            else if (val < thresh_2)
                                {return("#CCCC00")}
                            else
                                {return("#ff6633")}
                        }
                        else{
                            if(Likely < thresh_1 )
                                {return("#1aff1a")}
                            else if (Likely < thresh_2)
                                {return("#CCCC00")}
                            else if (Likely >= thresh_2)
                                {return("#ff6633")}
                        }

                    }()) %>;
                    color: #111111;font-size:2em;text-align:center">
                <%= value %>
                </div>
            """),
            'Definite': HTMLTemplateFormatter(template="""
                <div style="background-color:<%= 
                    (function colorfromint(){
                        var thresh_1 = 8;
                        var thresh_2 = 15;
                        if (typeof Definite == 'string'){
                            var val = Definite.split(' ');
                            val = val.slice(-1)[0] 
                            console.log(val);
                            if(val < thresh_1 )
                                {return("#1aff1a")}
                            else if (val < thresh_2)
                                {return("#CCCC00")}
                            else
                                {return("#ff6633")}
                        }
                        else{
                            if(Definite < thresh_1 )
                                {return("#1aff1a")}
                            else if (Definite < thresh_2)
                                {return("#CCCC00")}
                            else if (Definite >= thresh_2)
                                {return("#ff6633")}
                        }

                    }()) %>;
                    color: #111111;font-size:2em;text-align:center">
                <%= value %>
                </div>
            """),
        }
}
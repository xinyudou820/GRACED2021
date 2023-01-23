;================================================;
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
; ================================================;
begin
;-----------------set the labelbar tag------------------
boxVs2= new((/1,39/),float)
boxVs2(0,:) = (/1,3,5,9.5,14,17,20,30,40,55,70,110,150,225,300,475,650,825,1000,1500,2000,3000,4000,6000,8000,11500,15000,22500,30000,55000,80000,140000,200000,600000,1000000,1500000,2000000,2600000,3200000/)
;-----------------set the areas which we will zoom in--------------------
;regions = (/"China","USA","Europe","East_Africa","South_America"/)
   Lons = (/70.,-135.,-15.,-20.,-86./)
   Lone = (/150.,-70.,45.,60.,-30./)

   Lats = (/10.,23.2,38.,-38.,-56./)
   Late = (/55.,50.,60.,38.,15./)
;--------------start read the data we want ------------ 
  INFILE1     = addfile(DATA_DIR+"./average_total_2021.nc","r") 
  O3_MODEL_use= tofloat(INFILE1->emission)
  LAT         = INFILE1->latitude
  LON        = INFILE1->longitude

  O3_MODEL    = mask(O3_MODEL_use, O3_MODEL_use .le. 0.0, False)

  O3_MODEL!0 = "lat"
  O3_MODEL!1 = "lon"
  O3_MODEL&lat = LAT
  O3_MODEL&lon = LON

;;---------- start to draw ------------------------------
plot= new((/1/),graphic)
plot1= new((/1/),graphic)
plot2= new((/1/),graphic)
plot3= new((/1/),graphic)
plot4= new((/1/),graphic)
plot5= new((/1/),graphic)
;;-----------------> set up work station
  wk_type = "png"
  wks  = gsn_open_wks(wk_type,"Global_2021_world_total")
;;-----------------> set up work station
  gsn_define_colormap(wks,"cmocean_thermal")  ; choose color map
;; finish set up work station

;;  SET UP RESOURCES ---->>>>>>>>>>>

          res                      = True              ; plot mods desired
          res@gsnDraw              = True;False
          res@gsnFrame             = False
          res@gsnMaximize          = False  ;;; Panel set up gsnMaximize, so you have to turn it to False if you set up panel gsnMaximize
          res@gsnAddCyclic         = False
          res@gsnLeftString      =  "   "
          res@gsnRightString      =  "unit: kgC/d "
          res@gsnRightStringFontHeightF = 0.01

          res@cnFillOn             = True
          res@cnFillMode           = "CellFill" ;; use raster mode
          res@cnLinesOn            = False
          res@cnLineLabelsOn       =False 
         ;;   Labe font height
         res@tmXBLabelFontHeightF = 0.01
         res@tmYLLabelFontHeightF = 0.01
         res@tmBorderLineColor    = "black"
         res@tmXBLabelFontColor   = "black";"grey"
         res@tmYLLabelFontColor   = "black";"grey"
         
         res@tmYROn = False            ; Turn off right tickmarks.
         res@tmYLOn = False            ; Turn off right tickmarks.
         res@tmXTOn = False            ; Turn off top tickmarks.
         res@tmXBOn = False            ; Turn off top tickmarks.

         res@tmBorderThicknessF          = 4.;8. * wks@LTF
         res@tmXBMajorThicknessF         = 4.;6. * wks@LTF
         res@tmXBMinorThicknessF         = 4.;4. * wks@LTF
         res@tmYLMajorThicknessF         = 4.;6. * wks@LTF
         res@tmYLMinorThicknessF         = 4.;4. * wks@LTF

         ;;;; projections  set up
         res@mpLimitMode   = "LatLon"
         res@mpMinLonF     = LON(0)
         res@mpMaxLonF     = LON(dimsizes(LON)-1)
         res@mpMaxLatF     = LAT(0)
         res@mpMinLatF     = -60.;LAT(dimsizes(LAT)-1)
         res@sfXArray      = LON      ;;;;x-axis is longitude
         res@sfYArray      = LAT      ;;;;y-axis is latitude

         ;;; choose map dataset
         res@mpDataSetName         = "./database/Earth..4"   ; This new database contains
         res@mpDataBaseVersion     = "MediumRes"  ; Medium resolution database
         res@mpOutlineOn           = True         ; Turn on map outlines
         res@mpOutlineBoundarySets   = "National";(/"Brazil","China","India"/)
         res@mpNationalLineColor    = "black";"grey"
         res@mpNationalLineThicknessF = 1.2
         res@mpGeophysicalLineThicknessF	= 1.2
         res@mpFillOn              = False         ; Turn on map fill
         res@mpFillBoundarySets    = "National"
         res@mpGeophysicalLineColor = "black";"grey"

        ;;; legend set up
        res@lbOrientation               = "Horizontal";"Vertical"
        res@lbLabelStride               = 4.0
        res@lbLabelBarOn=True;False
        res@pmTickMarkDisplayMode       = "Always"
        res@pmLabelBarWidthF            = 0.55
        res@pmLabelBarHeightF           = 0.05

        res@gsnLeftString      =  "   "
        res@gsnRightString      =  "unit: kgC/d "
        res@gsnRightStringFontHeightF = 0.01
        res@gsnRightStringFontHeightF = 0.01
        res@gsnRightStringOrthogonalPosF = -1.1
        res@gsnRightStringParallelPosF   = 0.6

        ; set the font in this figure
        res@tiMainFont = 25
        res@lbLabelFont = 25
        res@txFont = 25

        res@cnLevelSelectionMode= "ExplicitLevels"
        colors = (/80,84,88,92,96,100,104,108,112,116,120,124,128,132,136,140,144,148,154,156,158,164,166,172,176,184,188,192,196,200,204,212,216,220,228,234,240,244,248,252/)

        res@cnLevels             = boxVs2(pp,:)
        res@lbLabelStrings       = boxVs2(pp,:) 
        res@cnFillColors         = colors(::-1)
        res@lbLabelFontHeightF   = 0.007
        res@lbBoxEndCapStyle            = "TriangleBothEnds"
;-------------
        res@vpXF                 = 0.4                  ; position and size
        res@vpYF                 = 0.62                 ; for contour plots

        res@vpWidthF             = 0.53
        res@vpHeightF            = 0.53


plot =  gsn_csm_contour_map(wks,O3_MODEL(:,:),res)

  res@gsnCenterStringFontHeightF = 0.014
  res@gsnCenterStringOrthogonalPosF = 0.03
;-China ---------------------------
  res@vpXF                 = 0.07                  ; position and size
  res@vpYF                 = 0.99                  ; for contour plots
  res@vpWidthF             = 0.41
  res@vpHeightF            = 0.75
  res@tmYROn = False            ; Turn off right tickmarks.
  res@tmYLOn = False            ; Turn off right tickmarks.
  res@tmXTOn = False            ; Turn off top tickmarks.
  res@tmXBOn = False
  res@lbLabelBarOn = False
  res@gsnCenterString      =  "South East Asia"
  res@gsnRightString      =  " "

  res@mpMinLonF     = Lons(0);LON(0)
  res@mpMaxLonF     = Lone(0);LON(dimsizes(LON)-1)
  res@mpMaxLatF     = Late(0);LAT(0)
  res@mpMinLatF     = Lats(0)    

plot1 = gsn_csm_contour_map(wks,O3_MODEL(:,:),res)

  res@vpXF                 = 0.07                 ; position and size
  res@vpYF                 = 0.505                  ; for contour plots
  res@vpWidthF             = 0.3
  res@vpHeightF            = 0.2

  res@mpMinLonF     = Lons(1);LON(0)
  res@mpMaxLonF     = Lone(1);LON(dimsizes(LON)-1)
  res@mpMaxLatF     = Late(1);LAT(0)
  res@mpMinLatF     = Lats(1)
  res@gsnCenterString      =  "U.S. mainland"

plot2 = gsn_csm_contour_map(wks,O3_MODEL(:,:),res) ;; USA


  res@vpXF                 = 0.07                 ; position and size
  res@vpYF                 = 0.36
  res@vpWidthF             = 0.3
  res@vpHeightF            = 0.2
  res@mpMinLonF     = Lons(2);LON(0)
  res@mpMaxLonF     = Lone(2);LON(dimsizes(LON)-1)
  res@mpMaxLatF     = Late(2);LAT(0)
  res@mpMinLatF     = Lats(2)
  res@gsnCenterString      =  "Europe"

plot3 = gsn_csm_contour_map(wks,O3_MODEL(:,:),res) ;;; Europe


  res@vpXF                 = 0.494                  ; position and size
  res@vpYF                 = 0.735                  ; for contour plots
  res@vpWidthF             = 0.243
  res@vpHeightF            = 0.24


  res@mpMinLonF     = Lons(3);LON(0)
  res@mpMaxLonF     = Lone(3);LON(dimsizes(LON)-1)
  res@mpMaxLatF     = Late(3);LAT(0)
  res@mpMinLatF     = Lats(3)
  res@gsnCenterString      =  "East Afria and Middle East"
plot4 = gsn_csm_contour_map(wks,O3_MODEL(:,:),res)

  res@mpMinLonF     = Lons(4);LON(0)
  res@mpMaxLonF     = Lone(4);LON(dimsizes(LON)-1)
  res@mpMaxLatF     = Late(4);LAT(0)
  res@mpMinLatF     = Lats(4)
  res@vpXF                 = 0.72                  ; position and size
  res@vpYF                 = 0.73                  ; for contour plots
  res@vpWidthF             = 0.24
  res@vpHeightF            = 0.23

   res@gsnCenterString      =  "South America"
plot5 = gsn_csm_contour_map(wks,O3_MODEL(:,:),res)
delete([/res@tmYROn,res@tmYLOn,res@tmXBOn,res@tmXTOn/])
;------add the rectangle -----------  

  gres                 = True
  gres@gsFillOpacityF     = 0.         
  gres@gsEdgesOn          = True       ; draw border around polygons
  gres@gsEdgeColor        = "grey40"
  gres@gsEdgeThicknessF   = 3.5
  do rr=0,4
  lat_rec = (/Lats(rr),Late(rr),Late(rr),Lats(rr)/)
  lon_rec = (/Lons(rr),Lons(rr),Lone(rr),Lone(rr)/)
  ploy1 = unique_string("ploygon")
  plot@$ploy1$ = gsn_add_polygon(wks,plot,lon_rec,lat_rec,gres)
  delete([/ploy1,lat_rec,lon_rec/])
  end do
;------------------
draw(plot)
frame(wks)

end
